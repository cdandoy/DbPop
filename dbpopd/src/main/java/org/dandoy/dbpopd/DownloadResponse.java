package org.dandoy.dbpopd;

import lombok.Getter;
import org.dandoy.dbpop.database.TableName;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Getter
public final class DownloadResponse {
    private final List<TableRowCount> tableRowCounts;
    private final int rowCount;

    public DownloadResponse(Map<TableName, Integer> rowCounts) {
        tableRowCounts = rowCounts.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().toQualifiedName()))
                .map(entry -> {
                    TableName tableName = entry.getKey();
                    Integer rowCount = entry.getValue();
                    return new TableRowCount(
                            tableName.toQualifiedName(),
                            tableName,
                            rowCount
                    );
                }).toList();
        rowCount = tableRowCounts.stream().map(TableRowCount::getRowCount).reduce(0, Integer::sum);
    }

    @Getter
    public static class TableRowCount {
        private final String displayName;
        private final TableName tableName;
        private final int rowCount;

        public TableRowCount(String displayName, TableName tableName, int rowCount) {
            this.displayName = displayName;
            this.tableName = tableName;
            this.rowCount = rowCount;
        }
    }
}
