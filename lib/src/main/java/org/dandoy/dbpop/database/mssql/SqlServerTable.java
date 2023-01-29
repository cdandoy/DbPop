package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlServerTable extends Table {
    private final String INDENT = "    ";

    public SqlServerTable(TableName tableName, List<Column> columns, List<Index> indexes, SqlServerPrimaryKey primaryKey, List<ForeignKey> foreignKeys) {
        super(tableName, columns, indexes, primaryKey, foreignKeys);
    }

    @Override
    public String tableDDL(Database database) {
        List<String> items = new ArrayList<>();

        for (Column column : getColumns()) {
            SqlServerColumn sqlServerColumn = (SqlServerColumn) column;
            items.add(sqlServerColumn.toDDL(database));
        }
        String pkDDL = pkDDL(database);
        if (pkDDL != null) {
            items.add(pkDDL);
        }

        return """
                CREATE TABLE %s
                (
                %s
                )
                """.formatted(
                database.quote(getTableName()),
                items.stream().map(s -> INDENT + s).collect(Collectors.joining(",\n"))
        );
    }

    private String pkDDL(Database database) {
        SqlServerPrimaryKey primaryKey = (SqlServerPrimaryKey) getPrimaryKey();
        if (primaryKey == null) return null;

        return primaryKey.toDDL(database);
    }
}
