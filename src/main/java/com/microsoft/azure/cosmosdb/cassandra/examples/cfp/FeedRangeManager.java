package com.microsoft.azure.cosmosdb.cassandra.examples.cfp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Manages feed ranges and associated page states using a Cassandra table in the same keyspace.
 *
 * Only one instance should update a given keyspace/table.
 * Only one thread should update a given range's page state.
 *
 * TODO: Add an application id to account for multiple sources and consumers.
 *
 * Expected schema.
 * CREATE TABLE change_feed_page_state(table_name text, range text, page_state text, PRIMARY KEY(table_name, range));
 */
public class FeedRangeManager {
    private final static String COMPLETE_RANGE_MARKER  = "complete";

    private final CqlSession session;
    private final String keyspace;
    private final String table;
    private final Map<String, String> pageStates = new HashMap<>();
    private PreparedStatement updatePageStatement;

    public FeedRangeManager(CqlSession session, String keyspace, String table) {
        this.session = session;
        this.keyspace = keyspace;
        this.table = table;
    }

    public void init(){

        session.execute("CREATE TABLE IF NOT EXISTS "+keyspace+".change_feed_page_state(table_name text, range text, page_state text, PRIMARY KEY(table_name, range))");

        String updatePageQuery = String.format("INSERT INTO %s.change_feed_page_state(table_name, range, page_state) values(?, ?, ?)", this.keyspace);
        this.updatePageStatement = session.prepare(updatePageQuery);

        String getPagesQuery = String.format("SELECT range, page_state from %s.change_feed_page_state WHERE table_name = ?", this.keyspace);
        BoundStatement getPagesStatement = session.prepare(getPagesQuery).bind(this.table);

        boolean hasRanges = false;
        boolean completeRanges = false;

        ResultSet results = session.execute(getPagesStatement);
        for (Row result : results) {
            hasRanges = true;
            String range = result.getString("range");
            String pageState = result.getString("page_state");
            if (range.equals(COMPLETE_RANGE_MARKER)){
                completeRanges = true;
            } else {
                pageStates.put(range, pageState);
            }
        }

        //Make sure this is either the first run for this table or all the ranges were loaded.
        //If a previous init failed to save a row for each range, data will be missed.
        if (hasRanges && !completeRanges){
            throw new RuntimeException("Detected an incomplete page state initialization.  Clear the table and restart.");
        }

        if (!hasRanges){
            List<String> ranges = fetchCurrentFeedRanges();
            for (String range : ranges) {
                this.updatePageState(range, null);
            }
            this.updatePageState(COMPLETE_RANGE_MARKER, String.valueOf(true));
        }
    }

    public String getPageState(String range){
        return pageStates.get(range);
    }

    public void updatePageState(String range, String state){
        BoundStatement statement = this.updatePageStatement.bind(this.table, range, state);
        session.execute(statement);
        if (!range.equals(COMPLETE_RANGE_MARKER)) {
            this.pageStates.put(range, state);
        }
    }

    public List<String> getAllFeedRanges(){
        //Order does not matter if workers are not distributed, but this could be modified to support distributed workers.
        return new ArrayList<>(this.pageStates.keySet());
    }

    private List<String> fetchCurrentFeedRanges()
    {
        PreparedStatement ps = session.prepare("SELECT range FROM system_cosmos.feedranges WHERE keyspace_name = ? AND table_name = ?");
        BoundStatement bs = ps.bind(this.keyspace, this.table);
        ResultSet results = session.execute(bs);
        return StreamSupport.stream(results.spliterator(), false)
                .map(r -> r.getString("range"))
                .collect(Collectors.toList());
    }
}
