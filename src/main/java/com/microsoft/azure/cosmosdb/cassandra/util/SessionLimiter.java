package com.microsoft.azure.cosmosdb.cassandra.util;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.microsoft.azure.cosmosdb.cassandra.examples.cfp.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionLimiter {
    private static final Logger log = LoggerFactory.getLogger(SessionLimiter.class.getName());
    final CqlSession session;
    final Semaphore semaphore;
    final int limit;

    public SessionLimiter(final CqlSession session, int limit) {
        System.out.println("Initializing SessionLimiter with limit=" + limit);
        this.session = session;
        this.limit = limit;
        semaphore = new Semaphore(limit);
    }

    public CompletionStage<AsyncResultSet> executeAsync(BoundStatement statement) throws InterruptedException {
        semaphore.acquire();
        CompletionStage<AsyncResultSet> future = session.executeAsync(statement);
        future.whenComplete((result, ex) ->
                {
                    semaphore.release();
                    if (ex != null) {
                        log.error("+++++++++++++++++++++");
                        log.error(ex.getMessage());
                    }
                }
        );
        return future;
    }

    public void waitForFinish() throws InterruptedException {
        while (semaphore.availablePermits() != limit) {
            Thread.sleep(200);
        }
    }
}
