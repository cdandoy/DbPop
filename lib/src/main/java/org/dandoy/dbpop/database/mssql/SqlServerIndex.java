package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Index;
import org.dandoy.dbpop.database.TableName;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

public class SqlServerIndex extends Index {
    private final String typeDesc;
    private final List<SqlServerIndexColumn> columns;

    public SqlServerIndex(String name, TableName tableName, boolean unique, boolean primaryKey, String typeDesc, List<SqlServerIndexColumn> columns) {
        super(name, tableName, unique, primaryKey, getIndexedColumns(columns));
        this.typeDesc = typeDesc;
        this.columns = columns;
    }

    @NotNull
    public static List<String> getIndexedColumns(List<SqlServerIndexColumn> sqlServerIndexColumns) {
        return sqlServerIndexColumns.stream()
                .filter(it -> !it.included())
                .map(it -> it.name)
                .toList();
    }

    public String toDDL(Database database) {
        List<String> includedColumnNames = columns.stream()
                .filter(SqlServerIndexColumn::included)
                .map(it -> database.quote(it.name))
                .toList();
        return "CREATE%s %s INDEX %s ON %s (%s)%s"
                .formatted(
                        isUnique() ? " UNIQUE" : "",
                        typeDesc,
                        database.quote(getName()),
                        database.quote(getTableName()),
                        super.getColumns().stream().map(database::quote).collect(Collectors.joining(", ")),
                        includedColumnNames.isEmpty() ? "" : " INCLUDE (" + String.join(", ", includedColumnNames) + ")"
                );
    }

    public record SqlServerIndexColumn(String name, boolean included) {}
}
