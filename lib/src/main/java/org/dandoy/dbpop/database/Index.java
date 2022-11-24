package org.dandoy.dbpop.database;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class Index {
    private final String name;
    private final TableName tableName;
    private final boolean unique;
    private final boolean primaryKey;
    private final List<String> columns;
}
