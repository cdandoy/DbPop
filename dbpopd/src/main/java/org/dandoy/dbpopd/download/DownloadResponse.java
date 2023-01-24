package org.dandoy.dbpopd.download;

import lombok.Getter;
import org.dandoy.dbpop.database.TableName;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Getter
public final class DownloadResponse {
    private final List<TableRowCount> tableRowCounts;
    private final boolean maxRowsReached;
    private final int rowCount;
    private final int rowsSkipped;

    public DownloadResponse(Map<TableName, Integer> rowCounts, Map<TableName, Integer> rowsSkipped, boolean maxRowsReached) {
        tableRowCounts = rowCounts.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().toQualifiedName()))
                .map(entry -> {
                    TableName tableName = entry.getKey();
                    Integer rowCount = entry.getValue();
                    return new TableRowCount(
                            tableName.toQualifiedName(),
                            tableName,
                            rowCount,
                            rowsSkipped.getOrDefault(tableName, 0)
                    );
                }).toList();
        this.maxRowsReached = maxRowsReached;
        this.rowCount = tableRowCounts.stream().map(TableRowCount::getRowCount).reduce(0, Integer::sum);
        this.rowsSkipped = tableRowCounts.stream().map(TableRowCount::getRowsSkipped).reduce(0, Integer::sum);
    }

    @Getter
    public static class TableRowCount {
        private final String displayName;
        private final TableName tableName;
        private final int rowCount;
        private final int rowsSkipped;

        public TableRowCount(String displayName, TableName tableName, int rowCount, int rowsSkipped) {
            this.displayName = displayName;
            this.tableName = tableName;
            this.rowCount = rowCount;
            this.rowsSkipped = rowsSkipped;
        }
    }
}
