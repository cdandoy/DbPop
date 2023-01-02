package org.dandoy.dbpop.download;

import lombok.Getter;
import org.dandoy.dbpop.database.TableName;

import java.util.HashMap;
import java.util.Map;

public class ExecutionContext {
    private final int totalRowCountLimit;
    private int totalRowCount;
    @Getter
    private final Map<TableName, Integer> rowCounts = new HashMap<>();

    public ExecutionContext() {
        this(Integer.MAX_VALUE);
    }

    public ExecutionContext(int totalRowCountLimit) {
        this.totalRowCountLimit = totalRowCountLimit;
    }

    public void tableAdded(TableName tableName) {
        rowCounts.putIfAbsent(tableName, 0);
    }

    public void rowAdded(TableName tableName) {
        Integer integer = rowCounts.get(tableName);
        if (integer == null) {
            integer = 0;
        }
        rowCounts.put(tableName, integer + 1);
        totalRowCount++;
    }

    public boolean keepRunning() {
        return totalRowCount < totalRowCountLimit;
    }
}
