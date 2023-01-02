package org.dandoy.dbpop.database;

import java.util.List;

public record Table(TableName tableName, List<Column> columns, List<Index> indexes, PrimaryKey primaryKey, List<ForeignKey> foreignKeys) {

    @Override
    public String toString() {
        return "Table{" +
               "tableName=" + tableName +
               '}';
    }

    public Column getColumn(String name) {
        return columns.stream().filter(column -> name.equals(column.getName())).findFirst().orElse(null);
    }
}
