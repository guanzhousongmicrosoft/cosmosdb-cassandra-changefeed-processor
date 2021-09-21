package com.microsoft.azure.cosmosdb.cassandra.examples.cfp;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.microsoft.azure.cosmosdb.cassandra.util.CassandraUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple change feed application that logs and counts rows.  It runs for 30 seconds and shuts down.
 */
public class ExampleApp extends ChangeFeedApp{

    private static final Logger log = LoggerFactory.getLogger(ExampleApp.class.getName());
    private final AtomicInteger numProcessed = new AtomicInteger(0);

    public ExampleApp(Session session,
                      String keyspace,
                      String table,
                      Timestamp startTime,
                      int pageSize,
                      int maxConcurrency,
                      int workerMinTime)
    {
        super(session, keyspace, table, startTime, pageSize, maxConcurrency, workerMinTime);
    }

    @Override
    public void stop() {
        super.stop();
        log.info("Graceful shutdown complete after processing {} rows", this.numProcessed.get());
    }

    @Override
    public void process(Row row){
        StringBuilder sb = new StringBuilder();
        for (ColumnDefinitions.Definition column : row.getColumnDefinitions()) {
            sb.append(column.getName());
            sb.append("=");
            sb.append(row.getObject(column.getName()));
            sb.append(", ");
        }

        log.info("Processed {}", sb.toString());
        this.numProcessed.incrementAndGet();
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();
        try(CassandraUtil util = new CassandraUtil(config)) {
            ExampleApp app = new ExampleApp(
                    util.getSession(),
                    config.getString("change_feed.keyspace"),
                    config.getString("change_feed.table"),
                    Timestamp.valueOf(config.getString("change_feed.start")),
                    config.getInt("change_feed.page_size"),
                    config.getInt("change_feed.concurrency"),
                    config.getInt("change_feed.min_execution_millis"));
            app.start();
            Thread.sleep(30000);
            app.stop();
        }
    }
}
