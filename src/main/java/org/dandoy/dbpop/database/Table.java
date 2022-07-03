package org.dandoy.dbpop.database;

import java.util.List;

public class Table {
    private final TableName tableName;
    private final List<Column> columns;
    private final List<Index> indexes;
    private final List<ForeignKey> foreignKeys;

    Table(TableName tableName, List<Column> columns, List<Index> indexes, List<ForeignKey> foreignKeys) {
        this.tableName = tableName;
        this.columns = columns;
        this.indexes = indexes;
        this.foreignKeys = foreignKeys;
    }

    @Override
    public String toString() {
        return "Table{" +
                "tableName=" + tableName +
                '}';
    }

    public TableName getTableName() {
        return tableName;
    }

    List<Column> getColumns() {
        return columns;
    }

    List<Index> getIndexes() {
        return indexes;
    }

    List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }
}
