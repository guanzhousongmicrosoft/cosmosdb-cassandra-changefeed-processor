package com.microsoft.azure.cosmosdb.cassandra.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public class TableIdentifier {
    private CqlIdentifier keyspace;
    private CqlIdentifier table;

    public void setKeyspace(CqlIdentifier keyspace) {
        this.keyspace = keyspace;
    }

    public void setTable(CqlIdentifier table) {
        this.table = table;
    }

    public CqlIdentifier getKeyspace() {
        return keyspace;
    }

    public CqlIdentifier getTable() {
        return table;
    }

    public TableIdentifier(CqlIdentifier keyspace, CqlIdentifier table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    public String toString() {
        return keyspace.asInternal() + "." + table.asInternal();
    }
}
