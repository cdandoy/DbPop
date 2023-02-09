package org.dandoy.dbpopd.content;

import lombok.Getter;
import org.dandoy.dbpop.database.RowCount;
import org.dandoy.dbpop.database.TableName;

import java.util.List;

@Getter
public class TableInfo implements Comparable<TableInfo> {
    private final TableName tableName;
    private final RowCount sourceRowCount;
    private final RowCount staticRowCount;
    private final RowCount baseRowCount;
    private final List<TableName> dependencies;

    public TableInfo(TableName tableName, RowCount sourceRowCount, RowCount staticRowCount, RowCount baseRowCount, List<TableName> dependencies) {
        this.tableName = tableName;
        this.sourceRowCount = sourceRowCount;
        this.staticRowCount = staticRowCount;
        this.baseRowCount = baseRowCount;
        this.dependencies = dependencies;
    }

    @Override
    public int compareTo(TableInfo that) {
        return this.tableName.compareTo(that.tableName);
    }
}