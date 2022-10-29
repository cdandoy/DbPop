package org.dandoy.dbpop.database;

import lombok.Getter;

@Getter
public class Column {
    private final String name;
    private final ColumnType columnType;
    private final boolean nullable;
    private final boolean autoIncrement;

    public Column(String name, ColumnType columnType, boolean nullable, boolean autoIncrement) {
        this.name = name;
        this.columnType = columnType;
        this.nullable = nullable;
        this.autoIncrement = autoIncrement;
    }

    @Override
    public String toString() {
        return autoIncrement ? name + " (identity)" : name;
    }
}
