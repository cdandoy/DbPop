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
    public String toDDL(Database database) {
        return tableDDL(database);
    }

    private String tableDDL(Database database) {
        List<String> items = new ArrayList<>();

        for (Column column : getColumns()) {
            SqlServerColumn sqlServerColumn = (SqlServerColumn) column;
            items.add(sqlServerColumn.toDDL(database));
        }
        String pkDDL = pkDDL(database);
        if (pkDDL != null) {
            items.add(pkDDL);
        }
        for (ForeignKey foreignKey : getForeignKeys()) {
            String fkDDL = fkDDL(database, foreignKey);
            items.add(fkDDL);
        }

        return """
                CREATE TABLE %s
                (
                %s
                )
                GO""".formatted(
                database.quote(getTableName()),
                items.stream().map(s -> INDENT + s).collect(Collectors.joining(",\n"))
        );
    }

    private String pkDDL(Database database) {
        SqlServerPrimaryKey primaryKey = (SqlServerPrimaryKey) getPrimaryKey();
        if (primaryKey == null) return null;

        return primaryKey.toDDL(database);
    }

    private String fkDDL(Database database, ForeignKey foreignKey) {
        return "CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)".formatted(
                database.quote(foreignKey.getName()),
                foreignKey.getFkColumns().stream().map(database::quote).collect(Collectors.joining(", ")),
                database.quote(foreignKey.getPkTableName()),
                foreignKey.getPkColumns().stream().map(database::quote).collect(Collectors.joining(", "))
        );
    }
}
