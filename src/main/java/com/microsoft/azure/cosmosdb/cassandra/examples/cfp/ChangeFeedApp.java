package com.microsoft.azure.cosmosdb.cassandra.examples.cfp;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An application will read from a Cassandra change feed using feed ranges.
 * It wil distribute the work to a configurable number of workers.
 */
public abstract class ChangeFeedApp {

    private static final Logger log = LoggerFactory.getLogger(ChangeFeedApp.class.getName());

    private final Session session;
    private final String keyspace;
    private final String table;
    private final Timestamp startTime;
    private final int pageSize;
    private final int maxConcurrency;
    private final int workerMinTime;
    private List<Thread> workerThreads;

    public ChangeFeedApp(Session session,
                         String keyspace,
                         String table,
                         Timestamp startTime,
                         int pageSize,
                         int maxConcurrency,
                         int workerMinTime)
    {
        this.session = session;
        this.keyspace = keyspace;
        this.table = table;
        this.startTime = startTime;
        this.pageSize = pageSize;
        this.maxConcurrency = maxConcurrency;
        this.workerMinTime = workerMinTime;
        this.workerThreads = null;
    }

    public void start()
    {
        FeedRangeManager feedRangeManager = new FeedRangeManager(session, this.keyspace, this.table);
        feedRangeManager.init();

        List<String> feedRanges = feedRangeManager.getAllFeedRanges();
        int numWorkers = Math.min(this.maxConcurrency, feedRanges.size());
        Map<Integer, List<String>> workerRanges = distributeFeedRanges(feedRangeManager.getAllFeedRanges(), numWorkers);

        this.workerThreads = new ArrayList<>(numWorkers);
        for (int workerId = 0; workerId < numWorkers; workerId++){
            Worker worker = new Worker(
                    feedRangeManager,
                    this::process,
                    this.session,
                    this.keyspace,
                    this.table,
                    workerRanges.get(workerId),
                    this.startTime,
                    pageSize,
                    workerMinTime);

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

    /**
     * The callback from the worker threads.  This will be called by multiple threads.
     *
     * @param row the row to process.
     */
    public abstract void process(Row row);

    private static Map<Integer, List<String>> distributeFeedRanges(List<String> allRanges, int numWorkers){
        Map<Integer, List<String>> workerRanges = new HashMap<>(numWorkers);

        for (int i = 0; i < allRanges.size(); i++) {
            int worker = i % numWorkers;
            List<String> currentRange = workerRanges.computeIfAbsent(worker, k -> new ArrayList<>(allRanges.size() / numWorkers + 1));

            currentRange.add((allRanges.get(i)));
        }

        return workerRanges;
    }
}
