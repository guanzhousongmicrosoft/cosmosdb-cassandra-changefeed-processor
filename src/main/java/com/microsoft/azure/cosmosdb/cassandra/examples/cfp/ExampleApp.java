package com.microsoft.azure.cosmosdb.cassandra.examples.cfp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.microsoft.azure.cosmosdb.cassandra.util.CassandraAPIUtil;
import com.microsoft.azure.cosmosdb.cassandra.util.MICassandraUtil;
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

    public ExampleApp(CqlSession session_API,
                      CqlSession session_MI,
                      Timestamp startTime,
                      int pageSize,
                      int maxConcurrency,
                      int workerMinTime)
    {
        super(session_API, session_MI ,startTime, pageSize, maxConcurrency, workerMinTime);
    }

    @Override
    public void stop() {
        super.stop();
        log.info("Graceful shutdown complete after processing rows");
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load();
        try(CassandraAPIUtil util_API = new CassandraAPIUtil(config);
            MICassandraUtil util_MI = new MICassandraUtil(config)) {
            ExampleApp app = new ExampleApp(
                    util_API.getSession(),
                    util_MI.getSession(),
                    Timestamp.valueOf(config.getString("change_feed.start")),
                    config.getInt("change_feed.page_size"),
                    config.getInt("change_feed.concurrency"),
                    config.getInt("change_feed.min_execution_millis"));
            app.start();
            Thread.sleep(3000000);
            app.stop();
        }
    }
}
