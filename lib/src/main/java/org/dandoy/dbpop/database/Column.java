package org.dandoy.dbpop.database;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;

@Getter
public class Column {
    private final String name;
    @JsonIgnore // Not sure how I would serialize that
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
