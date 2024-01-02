package com.microsoft.azure.cosmosdb.cassandra.examples.cfp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
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
    private final CqlSession session_API;
    private final CqlSession session_MI;
    private final String keyspace;
    private final String table;
    private final List<String> feedRanges;
    private final Timestamp startTime;
    private final int minExecutionMillis;
    private final int pageSize;
    private PreparedStatement preparedStatement;

    public Worker(FeedRangeManager feedRangeManager,
                  CqlSession session_API,
                  CqlSession session_MI,
                  String keyspace,
                  String table,
                  List<String> feedRanges,
                  Timestamp startTime,
                  int pageSize,
                  int minExecutionMillis) {
        this.feedRangeManager = feedRangeManager;
        this.session_API = session_API;
        this.session_MI = session_MI;
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
        String createTableQuery = "CREATE TABLE IF NOT EXISTS "+keyspace+"."+table+" (user_id UUID PRIMARY KEY, user_name text, user_bcity text)";
        session_MI.execute(createTableQuery);

        String changeFeedQuery = String.format("SELECT * FROM %s.%s WHERE COSMOS_CHANGEFEED_START_TIME() = ? AND COSMOS_FEEDRANGE() = ?", this.keyspace, this.table);
        this.preparedStatement = session_API.prepare(changeFeedQuery);
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

                this.processFeedRange(this.feedRanges.get(rangeIndex), this.session_MI);
                rangeIndex = rangeIndex >= this.feedRanges.size() - 1 ? 0 : rangeIndex + 1;
            }
        } catch (InterruptedException e){
            log.info("Worker interrupted. Gracefully shutting down.");
        } catch (Exception e){
            log.error("Worker failed with exception", e);
            throw e;
        }
    }

    private void processFeedRange(String range, CqlSession session_MI) throws InterruptedException {
        log.info("Processing feed range {}", range);
        BoundStatement boundStatement = this.preparedStatement.bind(this.startTime.toInstant(), range);
        boundStatement = boundStatement.setPageSize(this.pageSize);
        String startPage = this.feedRangeManager.getPageState(range);
        if (startPage != null){
            log.info("Got incoming page state {}", startPage);
            boundStatement = boundStatement.setPagingState( PagingState.fromString(startPage));
        }

        try{
            ResultSet results = session_API.execute(boundStatement);
            int rowsRemaining = results.getAvailableWithoutFetching();
            log.debug("rows available {}", results.getAvailableWithoutFetching());

            if (rowsRemaining > 0) {
                final int size = results.getColumnDefinitions().size();
                StringBuilder valueStringBuilder = new StringBuilder();
                String[] columns = new String[size];

                int column_index = 0;
                for(ColumnDefinition column : results.getColumnDefinitions()) {
                    String columnName = column.getName().asCql(false);
                    log.warn("+++++++++++++++++++++++++Column: {} {}", columnName, column.getType().asCql(false, false));
                    columns[column_index++] = columnName;
                    valueStringBuilder.append("?, ");
                }

                final String insertStatement = String.format("INSERT INTO  %s.%s (%s) VALUES (%s)",
                        this.keyspace,
                        this.table,
                        String.join(", ", columns),
                        valueStringBuilder.toString().replaceAll(", $", ""));

                PreparedStatement preparedInsertStatement = session_MI.prepare(insertStatement);

                List<Object> values = new ArrayList<>(size);
                for (Row row : results) {
                    for (int i = 0; i < size; i++) {
                        values.add(row.getObject(columns[i]));
                    }

                    BoundStatement bound = preparedInsertStatement.bind(values.toArray());
                    this.session_MI.execute(bound);
                    values.clear();
                    /*
                     * Add your own business logic here to process rows that appears in the change feed....
                     */
                    if (--rowsRemaining == 0) {
                        break;
                    }
                }
            }

            //Make sure results are processed before setting checkpoint
            this.feedRangeManager.updatePageState(range, results.getExecutionInfo().getSafePagingState().toString());
        } catch (OverloadedException e){
            log.warn("Possible TooManyRequests (429), Skipping range {} for now and retry it later", range);
            log.warn("Exception: ", e);
            Thread.sleep(5 * 1000);
        }
    }
}
