package org.dandoy.dbpop.database;

import java.util.Collections;
import java.util.List;

public record PrimaryKey(String name, List<String> columns) {
    public PrimaryKey(String name, List<String> columns) {
        this.name = name;
        this.columns = Collections.unmodifiableList(columns);
    }
}
