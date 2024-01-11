package com.microsoft.azure.cosmosdb.cassandra.examples.cfp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.microsoft.azure.cosmosdb.cassandra.util.CassandraAPIUtil;
import com.microsoft.azure.cosmosdb.cassandra.util.TableIdentifier;
import com.microsoft.azure.cosmosdb.cassandra.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger log = LoggerFactory.getLogger(FeedRangeManager.class.getName());
    private final static String COMPLETE_RANGE_MARKER  = "complete";

    private final CqlSession session;
    private final Set<TableIdentifier> tables;

    private final Map<TableIdentifier, Map<String, String>> pageStates;
    private Map<TableIdentifier, PreparedStatement> updatePageStatements;

    public FeedRangeManager(CqlSession session, Set<TableIdentifier> tables){
        this.session = session;
        this.tables = tables;

        pageStates = new HashMap<>();
        updatePageStatements = new HashMap<>();
    }

    public void init(){
        for (TableIdentifier table : tables) {
            initTable(table);
        }
    }

    public void initTable(TableIdentifier tableIdentifier){

        session.execute("CREATE TABLE IF NOT EXISTS "+ tableIdentifier.getKeyspace() +".change_feed_page_state(table_name text, range text, page_state text, PRIMARY KEY(table_name, range))");

        String updatePageQuery = String.format("INSERT INTO %s.change_feed_page_state(table_name, range, page_state) values(?, ?, ?)", tableIdentifier.getKeyspace());
        PreparedStatement updatePageStatements = session.prepare(updatePageQuery);
        this.updatePageStatements.put(tableIdentifier, updatePageStatements);

        String getPagesQuery = String.format("SELECT range, page_state from %s.change_feed_page_state WHERE table_name = ?", tableIdentifier.getKeyspace());
        BoundStatement getPagesStatement = session.prepare(getPagesQuery).bind(tableIdentifier.getTable().asInternal());

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
                this.pageStates.put(tableIdentifier, new HashMap<>());
                this.pageStates.get(tableIdentifier).put(range, pageState);
            }
        }

        //Make sure this is either the first run for this table or all the ranges were loaded.
        //If a previous init failed to save a row for each range, data will be missed.
        if (hasRanges && !completeRanges){
            throw new RuntimeException("Detected an incomplete page state initialization.  Clear the table and restart.");
        }

        if (!hasRanges){
            List<String> ranges = fetchCurrentFeedRanges(tableIdentifier);
            for (String range : ranges) {
                this.updatePageState(tableIdentifier, range, null);
            }
            this.updatePageState(tableIdentifier, COMPLETE_RANGE_MARKER, String.valueOf(true));
        }
    }

    public String getPageState(TableIdentifier tableIdentifier, String range){
        return pageStates.getOrDefault(tableIdentifier, new HashMap<>()).getOrDefault(range, "");
    }

    public void updatePageState(TableIdentifier tableIdentifier, String range, String state){
        BoundStatement statement = this.updatePageStatements.get(tableIdentifier).bind(tableIdentifier.getTable().asInternal(), range, state);
        session.execute(statement);
        if (!range.equals(COMPLETE_RANGE_MARKER)) {
            if(this.pageStates.containsKey(tableIdentifier)){
                this.pageStates.get(tableIdentifier).put(range, state);
            } else {
                this.pageStates.put(tableIdentifier, new HashMap<>());
                this.pageStates.get(tableIdentifier).put(range, state);
            }
        }
    }

    public List<String> getAllFeedRanges(TableIdentifier tableIdentifier){
        //Order does not matter if workers are not distributed, but this could be modified to support distributed workers.
        return new ArrayList<>(this.pageStates.getOrDefault(tableIdentifier, new HashMap<>()).keySet());
    }

    public List<Tuple<TableIdentifier, String>> getAllFeedRanges(Set<TableIdentifier> tableIdentifiers){
        //Order does not matter if workers are not distributed, but this could be modified to support distributed workers.
        List<Tuple<TableIdentifier, String>> ranges = new ArrayList<>();
        for (TableIdentifier tableIdentifier : tableIdentifiers) {
            for (String range : this.pageStates.getOrDefault(tableIdentifier, new HashMap<>()).keySet()) {
                ranges.add(new Tuple<>(tableIdentifier, range));
            }
        }
        return ranges;
    }

    private List<String> fetchCurrentFeedRanges(TableIdentifier tableIdentifier)
    {
        PreparedStatement ps = session.prepare("SELECT range FROM system_cosmos.feedranges WHERE keyspace_name = ? AND table_name = ?");
        BoundStatement bs = ps.bind(tableIdentifier.getKeyspace().asInternal(), tableIdentifier.getTable().asInternal());
        log.info("Fetching feed ranges for keyspace: {}, table: {}", tableIdentifier.getKeyspace().asInternal(), tableIdentifier.getTable().asInternal());
        ResultSet results = session.execute(bs);
        return StreamSupport.stream(results.spliterator(), false)
                .map(r -> r.getString("range"))
                .collect(Collectors.toList());
    }
}
