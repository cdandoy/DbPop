package org.dandoy.dbpop.database;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableName implements Comparable<TableName> {
    private final String catalog;
    private final String schema;
    private final String table;

    @JsonCreator
    public TableName(
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table
    ) {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
    }

    @Override
    public String toString() {
        return "TableName{" +
               "catalog='" + catalog + '\'' +
               ", schema='" + schema + '\'' +
               ", table='" + table + '\'' +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableName tableName = (TableName) o;
        return Objects.equals(catalog, tableName.catalog) && Objects.equals(schema, tableName.schema) && table.equals(tableName.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table);
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String toQualifiedName() {
        return Stream.of(catalog, schema, table).filter(Objects::nonNull).collect(Collectors.joining("."));
    }

    @Override
    public int compareTo(@NotNull TableName that) {
        int ret = this.getCatalog().compareTo(that.getCatalog());
        if (ret == 0) {
            ret = this.getSchema().compareTo(that.getSchema());
        }
        if (ret == 0) {
            ret = this.getTable().compareTo(that.getTable());
        }
        return ret;

    }
}
