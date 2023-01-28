package org.dandoy.dbpop.database;

import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@Getter
public class Index {
    private final String name;
    private final TableName tableName;
    private final boolean unique;
    private final boolean primaryKey;
    private final List<String> columns;

    public Index(String name, TableName tableName, boolean unique, boolean primaryKey, List<String> columns) {
        this.name = name;
        this.tableName = tableName;
        this.unique = unique;
        this.primaryKey = primaryKey;
        this.columns = columns;
    }

    public String toDDL(Database database) {
        return "CREATE%s INDEX %s ON %s (%s)"
                .formatted(
                        unique ? " UNIQUE" : "",
                        database.quote(name),
                        database.quote(tableName),
                        columns.stream().map(database::quote).collect(Collectors.joining(", "))
                );
    }
}
