package com.microsoft.azure.cosmosdb.cassandra.examples.cfp;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The example worker loops through all of it's assigned ranges and logging the
 * data it retrieves.
 * The page state is saved to a separate C* table between queries.
 */
public class Worker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Worker.class.getName());

    private final FeedRangeManager feedRangeManager;
    private final Consumer<Row> rowProcessor;
    private final Session session;
    private final String keyspace;
    private final String table;
    private final List<String> feedRanges;
    private final Timestamp startTime;
    private final int minExecutionMillis;
    private final int pageSize;
    private PreparedStatement preparedStatement;

    public Worker(FeedRangeManager feedRangeManager,
                  Consumer<Row> rowProcessor,
                  Session session,
                  String keyspace,
                  String table,
                  List<String> feedRanges,
                  Timestamp startTime,
                  int pageSize,
                  int minExecutionMillis) {
        this.feedRangeManager = feedRangeManager;
        this.rowProcessor = rowProcessor;
        this.session = session;
        this.keyspace = keyspace;
        this.table = table;
        this.feedRanges = feedRanges;
        this.startTime = startTime;
        this.pageSize = pageSize;
        this.minExecutionMillis = minExecutionMillis;
        this.preparedStatement = null;
    }

    public void run() {
        int rangeIndex = 0;
        long rotationStarted = -1;
        String changeFeedQuery = String.format("SELECT * FROM %s.%s WHERE COSMOS_CHANGEFEED_START_TIME() = ? AND COSMOS_FEEDRANGE() = ?", this.keyspace, this.table);
        this.preparedStatement = session.prepare(changeFeedQuery);
        log.info("Starting worker for feed ranges {}", feedRanges);
        try {
            while (!Thread.currentThread().isInterrupted()) {
                if (rangeIndex == 0){
                    long now = System.nanoTime();
                    long rotationTime = rotationStarted == -1 ? -1 : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - rotationStarted);
                    if ( rotationTime >= 0 && rotationTime < this.minExecutionMillis){
                        long sleepTime = this.minExecutionMillis - rotationTime;

                        //Sleep in case the result sets are empty, to avoid a lot of unnecessary requests.
                        log.debug("Rotation took {} of {}. Sleeping for {}", rotationTime, this.minExecutionMillis, sleepTime);
                        Thread.sleep(sleepTime);
                    }

                    rotationStarted = now;
                }

                this.processFeedRange(this.feedRanges.get(rangeIndex));
                rangeIndex = rangeIndex >= this.feedRanges.size() - 1 ? 0 : rangeIndex + 1;
            }
        } catch (InterruptedException e){
            log.info("Worker interrupted. Gracefully shutting down.");
        } catch (Exception e){
            log.error("Worker failed with exception", e);
            throw e;
        }
    }

    private void processFeedRange(String range){
        log.info("Processing feed range {}", range);
        BoundStatement boundStatement = this.preparedStatement.bind(this.startTime, range);
        boundStatement.setFetchSize(this.pageSize);
        String startPage = this.feedRangeManager.getPageState(range);
        if (startPage != null){
            log.info("Got incoming page state {}", startPage);
            boundStatement.setPagingState(PagingState.fromString(startPage));
        }

        ResultSet results = session.execute(boundStatement);
        int rowsRemaining = results.getAvailableWithoutFetching();
        log.debug("rows available {}", results.getAvailableWithoutFetching());

        
        if (rowsRemaining > 0) {
            for (Row row : results) {
                this.rowProcessor.accept(row);    
                /*
                * Add your own business logic here to process rows that appears in the change feed....
                */        
                if (--rowsRemaining == 0) {
                    break;
                }
            }
        }

        //Make sure results are processed before setting checkpoint
        this.feedRangeManager.updatePageState(range, results.getExecutionInfo().getPagingState().toString());
    }
}
