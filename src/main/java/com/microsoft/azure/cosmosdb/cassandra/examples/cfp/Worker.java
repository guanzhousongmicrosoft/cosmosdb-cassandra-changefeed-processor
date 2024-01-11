package com.microsoft.azure.cosmosdb.cassandra.examples.cfp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.microsoft.azure.cosmosdb.cassandra.util.MICassandraUtil;
import com.microsoft.azure.cosmosdb.cassandra.util.TableIdentifier;
import com.microsoft.azure.cosmosdb.cassandra.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
    private final List<Tuple<TableIdentifier, String>> feedRanges;
    private final Timestamp startTime;
    private final int minExecutionMillis;
    private final int pageSize;
    private AtomicInteger numProcessed;
    private Map<TableIdentifier, PreparedStatement> preparedStatementMap;
    private final int batchSize = 10;

    public Worker(FeedRangeManager feedRangeManager,
                  CqlSession session_API,
                  CqlSession session_MI,
                  List<Tuple<TableIdentifier, String>> feedRanges,
                  Timestamp startTime,
                  int pageSize,
                  int minExecutionMillis,
                  AtomicInteger numProcessed) {
        this.feedRangeManager = feedRangeManager;
        this.session_API = session_API;
        this.session_MI = session_MI;
        this.feedRanges = feedRanges;
        this.startTime = startTime;
        this.pageSize = pageSize;
        this.minExecutionMillis = minExecutionMillis;
        this.preparedStatementMap = new HashMap<>();
        this.numProcessed = numProcessed;
    }

    public void run() {
        int rangeIndex = 0;
        long rotationStarted = -1;

        for(Tuple<TableIdentifier, String> feedRange : this.feedRanges){
            TableIdentifier tableIdentifier = feedRange.x;
            String range = feedRange.y;
            String changeFeedQuery = String.format("SELECT * FROM %s.%s WHERE COSMOS_CHANGEFEED_START_TIME() = ? AND COSMOS_FEEDRANGE() = ?",
                    tableIdentifier.getKeyspace().asInternal(),
                    tableIdentifier.getTable().asInternal());

            PreparedStatement preparedStatement = session_API.prepare(changeFeedQuery);
            this.preparedStatementMap.put(tableIdentifier, preparedStatement);
            log.info("Prepared worker for feed ranges {}", feedRanges);
        }

        try {
            while (!Thread.currentThread().isInterrupted()) {
                log.info("Processed total rows: {} Time: {} ", new Timestamp(System.currentTimeMillis()), this.numProcessed.get());
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
                log.info("Processed row: {}", this.numProcessed.get());
            }
        } catch (InterruptedException e){
            log.info("Worker interrupted. Gracefully shutting down.");
        } catch (Exception e){
            log.error("Worker failed with exception", e);
            throw e;
        }
    }

    private void processFeedRange(Tuple<TableIdentifier, String> tableRange, CqlSession session_MI) throws InterruptedException {
        TableIdentifier tableIdentifier = tableRange.x;
        String range = tableRange.y;
        log.info("Processing keyspace {} table {} with feed range {}", tableIdentifier.getKeyspace().asInternal(), tableIdentifier.getTable().asInternal(), range);

        BoundStatement boundStatement = this.preparedStatementMap.get(tableIdentifier).bind(this.startTime.toInstant(), range);
        boundStatement = boundStatement.setPageSize(this.pageSize);
        String startPage = this.feedRangeManager.getPageState(tableIdentifier, range);
        if (startPage != null){
            //log.info("Got incoming page state {}", startPage);
            boundStatement = boundStatement.setPagingState(PagingState.fromString(startPage));
        }

        try{
            ResultSet results = session_API.execute(boundStatement);
            int resultSize = results.getAvailableWithoutFetching();
            log.debug("rows available {}", results.getAvailableWithoutFetching());

            if (resultSize > 0) {
                final int size = results.getColumnDefinitions().size();
                StringBuilder valueStringBuilder = new StringBuilder();
                String[] columns = new String[size];

                int column_index = 0;
                for(ColumnDefinition column : results.getColumnDefinitions()) {
                    String columnName = column.getName().asInternal();
                    //log.warn("+++++++++++++++++++++++++Column: {} {}", columnName, column.getType().asCql(false, false));
                    columns[column_index++] = columnName;
                    valueStringBuilder.append("?, ");
                }

                final String insertStatement = String.format("INSERT INTO  %s.%s (%s) VALUES (%s)",
                        tableIdentifier.getKeyspace().asInternal(),
                        tableIdentifier.getTable().asInternal(),
                        String.join(", ", columns),
                        valueStringBuilder.toString().replaceAll(", $", ""));

                PreparedStatement preparedInsertStatement = session_MI.prepare(insertStatement);

                log.info("Got {} results for keyspace {} table {} with feed range {}", resultSize, tableIdentifier.getKeyspace().asInternal(), tableIdentifier.getTable().asInternal(), range);


                int rowsRemaining = resultSize;
                List<BoundStatement> boundStatements = new ArrayList<>(batchSize);

                for (Row row : results) {
                    rowsRemaining--;
                    List<Object> values = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        values.add(row.getObject(columns[i]));
                    }

                    boundStatements.add(preparedInsertStatement.bind(values.toArray()));

                    if(boundStatements.size() == batchSize || rowsRemaining == 0){
                        BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.LOGGED, boundStatements.toArray(new BoundStatement[0]));
                        session_MI.execute(batch);
                    }

                    //log.info("prepared row {}: {}", rowsRemaining, values);
                    if (rowsRemaining == 0) {
                        break;
                    }
                }

                this.numProcessed.addAndGet(resultSize);
            }

            //Make sure results are processed before setting checkpoint
            this.feedRangeManager.updatePageState(tableIdentifier, range, results.getExecutionInfo().getSafePagingState().toString());
        } catch (OverloadedException e){
            log.warn("Possible TooManyRequests (429), Skipping range {} for now and retry it later", range);
            log.warn("Exception: ", e);
            Thread.sleep(5 * 1000);
        }
    }
}
