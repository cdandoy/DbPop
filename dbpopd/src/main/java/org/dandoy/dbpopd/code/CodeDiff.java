package org.dandoy.dbpopd.code;

import org.dandoy.dbpop.database.TableName;

import java.util.List;

public record CodeDiff(List<Entry> entries) {
    record Entry(TableName tableName, String type, Long databaseTime, Long fileTime) {}
}
