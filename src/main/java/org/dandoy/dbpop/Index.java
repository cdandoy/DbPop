package org.dandoy.dbpop;

import java.util.List;

public class Index {
    private final String name;
    private final TableName tableName;
    private final boolean unique;
    private final boolean primaryKey;
    private final String filter;
    private final List<String> columns;

    public Index(String name, TableName tableName, boolean unique, boolean primaryKey, String filter, List<String> columns) {
        this.name = name;
        this.tableName = tableName;
        this.unique = unique;
        this.primaryKey = primaryKey;
        this.filter = filter;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    public TableName getTableName() {
        return tableName;
    }

    public boolean isUnique() {
        return unique;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public String getFilter() {
        // TODO: Implement filtered indexes?
        return filter;
    }

    public List<String> getColumns() {
        return columns;
    }
}
