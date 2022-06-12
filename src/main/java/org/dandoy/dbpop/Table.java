package org.dandoy.dbpop;

import java.util.List;

public class Table {
    private final TableName tableName;
    private final List<Column> columns;
    private final List<Index> indexes;
    private final List<ForeignKey> foreignKeys;

    public Table(TableName tableName, List<Column> columns, List<Index> indexes, List<ForeignKey> foreignKeys) {
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

    public List<Column> getColumns() {
        return columns;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }
}
