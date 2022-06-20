package org.dandoy.dbpop;

import java.util.List;

class Index {
    private final String name;
    private final TableName tableName;
    private final boolean unique;
    private final boolean primaryKey;
    private final List<String> columns;

     Index(String name, TableName tableName, boolean unique, boolean primaryKey, List<String> columns) {
        this.name = name;
        this.tableName = tableName;
        this.unique = unique;
        this.primaryKey = primaryKey;
         this.columns = columns;
    }

     String getName() {
        return name;
    }

     TableName getTableName() {
        return tableName;
    }

     boolean isUnique() {
        return unique;
    }

     boolean isPrimaryKey() {
        return primaryKey;
    }

    public List<String> getColumns() {
        return columns;
    }
}
