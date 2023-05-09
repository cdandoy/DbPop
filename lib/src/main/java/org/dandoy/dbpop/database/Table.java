package org.dandoy.dbpop.database;

import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
public class Table {
    private final TableName tableName;
    private final List<Column> columns;
    private final List<Index> indexes;
    private final PrimaryKey primaryKey;
    private final List<ForeignKey> foreignKeys;

    public Table(TableName tableName, List<Column> columns, List<Index> indexes, PrimaryKey primaryKey, List<ForeignKey> foreignKeys) {
        this.tableName = tableName;
        this.columns = columns;
        this.indexes = indexes;
        this.primaryKey = primaryKey;
        this.foreignKeys = foreignKeys;
    }

    @Override
    public String toString() {
        return "Table{" +
               "tableName=" + tableName +
               '}';
    }

    public Column getColumn(String name) {
        return columns.stream().filter(column -> name.equals(column.getName())).findFirst().orElse(null);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Table) obj;
        return Objects.equals(this.tableName, that.tableName) &&
               Objects.equals(this.columns, that.columns) &&
               Objects.equals(this.indexes, that.indexes) &&
               Objects.equals(this.primaryKey, that.primaryKey) &&
               Objects.equals(this.foreignKeys, that.foreignKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columns, indexes, primaryKey, foreignKeys);
    }

    public String tableDDL(Database database) {
        throw new RuntimeException("Not implemented");
    }

    public static String getForeignKeyDefinition(Database database, TableName tableName, ForeignKey foreignKey) {
        return "ALTER TABLE %s ADD %s".formatted(
                database.quote(tableName),
                foreignKey.toDDL(database)
        );
    }
}
