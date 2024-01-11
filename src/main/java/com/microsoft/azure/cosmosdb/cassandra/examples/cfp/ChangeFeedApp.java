package com.microsoft.azure.cosmosdb.cassandra.examples.cfp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.microsoft.azure.cosmosdb.cassandra.util.MICassandraUtil;
import com.microsoft.azure.cosmosdb.cassandra.util.TableIdentifier;
import com.microsoft.azure.cosmosdb.cassandra.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * An application will read from a Cassandra change feed using feed ranges.
 * It wil distribute the work to a configurable number of workers.
 */
public abstract class ChangeFeedApp {

    private static final Logger log = LoggerFactory.getLogger(ChangeFeedApp.class.getName());

    private final CqlSession session_API;
    private final CqlSession session_MI;
    private final Timestamp startTime;
    private final int pageSize;
    private final int maxConcurrency;
    private final int workerMinTime;
    private List<Thread> workerThreads;
    private AtomicInteger numProcessed = new AtomicInteger(0);
    private Set<TableIdentifier> tables;

    public ChangeFeedApp(CqlSession session_API,
                         CqlSession session_MI,
                         Timestamp startTime,
                         int pageSize,
                         int maxConcurrency,
                         int workerMinTime)
    {
        this.session_API = session_API;
        this.session_MI = session_MI;
        this.startTime = startTime;
        this.pageSize = pageSize;
        this.maxConcurrency = maxConcurrency;
        this.workerMinTime = workerMinTime;
        this.workerThreads = null;
    }

    public void start()
    {
        this.tables = MICassandraUtil.getAllKeyspacesAndTables(session_API);
        this.tables = this.tables.stream().filter(t -> !t.getTable().asInternal().equalsIgnoreCase("change_feed_page_state")).collect(Collectors.toSet());

        for(TableIdentifier tableIdentifier : this.tables){
            log.info("Table: {}", tableIdentifier.getTable());
            MICassandraUtil.createKeyspaceAndTableIfNotExists(session_API, session_MI,
                    tableIdentifier.getKeyspace().asInternal(),
                    tableIdentifier.getTable().asInternal());
        }

        FeedRangeManager feedRangeManager = new FeedRangeManager(session_API, this.tables);
        feedRangeManager.init();

        List<Tuple<TableIdentifier, String>> feedRanges = feedRangeManager.getAllFeedRanges(this.tables);
        int numWorkers = Math.min(this.maxConcurrency, feedRanges.size());

        Map<Integer, List<Tuple<TableIdentifier, String>>> workerRanges = distributeFeedRanges(feedRanges, numWorkers);


        this.workerThreads = new ArrayList<>(numWorkers);
        for (int workerId = 0; workerId < numWorkers; workerId++){
            Worker worker = new Worker(
                    feedRangeManager,
                    this.session_API,
                    this.session_MI,
                    workerRanges.get(workerId),
                    this.startTime,
                    pageSize,
                    workerMinTime,
                    numProcessed);

            Thread workerThread = new Thread(worker);
            workerThread.setDaemon(true);
            workerThread.setName("ChangeFeed Processor " + workerId);

            //Fail the app if any worker fails.
            workerThread.setUncaughtExceptionHandler((t, e) -> this.stop());
            workerThreads.add(workerThread);
            workerThread.start();
        }
    }

    public void stop(){
        try {
            if (this.workerThreads != null) {
                for (Thread workerThread : workerThreads) {
                    log.debug("Stopping {}", workerThread);
                    workerThread.interrupt();
                }

                for (Thread workerThread : workerThreads) {
                    log.debug("Waiting for {} to finish", workerThread);
                    workerThread.join();
                }
            }
        } catch (InterruptedException e){
            log.error("Interrupted waiting for shutdown", e);
        }
    }

    private static Map<Integer, List<Tuple<TableIdentifier, String>>> distributeFeedRanges(List<Tuple<TableIdentifier, String>> allRanges, int numWorkers){
        Map<Integer, List<Tuple<TableIdentifier, String>>> workerRanges = new HashMap<>(numWorkers);

        for (int i = 0; i < allRanges.size(); i++) {
            int worker = i % numWorkers;
            List<Tuple<TableIdentifier, String>> currentRange = workerRanges.computeIfAbsent(worker, k -> new ArrayList<>(allRanges.size() / numWorkers + 1));

            currentRange.add((allRanges.get(i)));
        }

        return workerRanges;
    }
}
