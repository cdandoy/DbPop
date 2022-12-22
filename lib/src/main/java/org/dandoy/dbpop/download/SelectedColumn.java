package org.dandoy.dbpop.download;

import org.dandoy.dbpop.database.ColumnType;

import java.util.Collection;

public record SelectedColumn(int jdbcPos, String name, ColumnType columnType, boolean binary) {
    public String asHeaderName() {
        if (binary) return name + "*b64";
        return name;
    }

    public static SelectedColumn findByName(Collection<SelectedColumn> selectedColumns, String columnName) {
        for (SelectedColumn selectedColumn : selectedColumns) {
            if (columnName.equals(selectedColumn.name)) {
                return selectedColumn;
            }
        }
        return null;
    }
}
