package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.Settings;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.database.utils.ForeignKeyCollector;
import org.dandoy.dbpop.database.utils.IndexCollector;
import org.dandoy.dbpop.database.utils.TableCollector;
import org.dandoy.dbpop.upload.DataFileHeader;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class SqlServerDatabase extends Database {
    public SqlServerDatabase(Connection connection) {
        super(connection);
    }

    private void use(String catalog) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("USE " + catalog);
        }
    }

    @Override
    public Collection<TableName> getTableNames(String catalog, String schema) {
        try {
            use(catalog);
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT s.name AS schema_name, t.name AS table_name FROM sys.schemas s JOIN sys.tables t ON s.schema_id = t.schema_id WHERE s.name = ?")) {
                preparedStatement.setString(1, schema);
                List<TableName> tableNames = new ArrayList<>();
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        tableNames.add(
                                new TableName(
                                        catalog,
                                        resultSet.getString("schema_name"),
                                        resultSet.getString("table_name")
                                )
                        );
                    }
                }
                return tableNames;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public Collection<Table> getTables(Set<TableName> datasetTableNames) {
        Set<String> catalogs = datasetTableNames.stream().map(TableName::getCatalog).collect(Collectors.toSet());
        Map<TableName, List<Column>> tableColumns = new HashMap<>();
        Map<TableName, List<ForeignKey>> foreignKeys = new HashMap<>();
        Map<TableName, List<Index>> indexes = new HashMap<>();
        try (PreparedStatement databasesStatement = connection.prepareStatement("SELECT name FROM sys.databases WHERE name NOT IN ('tempdb')")) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet databaseResultSet = databasesStatement.executeQuery()) {
                    while (databaseResultSet.next()) {
                        String catalog = databaseResultSet.getString("name");
                        if (catalogs.contains(catalog)) {
                            statement.execute("USE " + catalog);
                            // Collects the tables and columns
                            try (PreparedStatement tablesStatement = connection.prepareStatement("" +
                                    "SELECT s.name   AS s,\n" +
                                    "       t.name   AS t,\n" +
                                    "       c.name   AS c,\n" +
                                    "       c.is_nullable,\n" +
                                    "       c.is_identity,\n" +
                                    "       ty.name  AS type_name,\n" +
                                    "       ty.scale AS type_scale\n" +
                                    "FROM sys.schemas s\n" +
                                    "         JOIN sys.tables t ON t.schema_id = s.schema_id\n" +
                                    "         JOIN sys.columns c ON c.object_id = t.object_id\n" +
                                    "         LEFT JOIN sys.types ty ON ty.system_type_id = c.system_type_id\n" +
                                    "ORDER BY s.name, t.name, c.column_id")) {
                                try (TableCollector tableCollector = new TableCollector((schema, table, columns) -> {
                                    TableName tableName = new TableName(catalog, schema, table);
                                    if (datasetTableNames.contains(tableName)) {
                                        tableColumns.put(tableName, columns);
                                    }
                                })) {
                                    try (ResultSet tablesResultSet = tablesStatement.executeQuery()) {
                                        while (tablesResultSet.next()) {
                                            String schema = tablesResultSet.getString("s");
                                            String table = tablesResultSet.getString("t");
                                            TableName tableName = new TableName(catalog, schema, table);
                                            if (datasetTableNames.contains(tableName)) {
                                                String column = tablesResultSet.getString("c");
                                                boolean nullable = tablesResultSet.getBoolean("is_nullable");
                                                boolean identity = tablesResultSet.getBoolean("is_identity");
                                                String typeName = tablesResultSet.getString("type_name");
                                                int typeScale = tablesResultSet.getInt("type_scale");
                                                ColumnType columnType = getColumnType(typeName, typeScale);
                                                tableCollector.push(
                                                        schema,
                                                        table,
                                                        new Column(
                                                                column,
                                                                columnType,
                                                                nullable,
                                                                identity
                                                        )
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            // Collects the indexes
                            try (PreparedStatement preparedStatement = connection.prepareStatement("\n" +
                                    "SELECT s.name AS s,\n" +
                                    "       t.name AS t,\n" +
                                    "       i.name AS i,\n" +
                                    "       i.is_unique,\n" +
                                    "       i.is_primary_key,\n" +
                                    "       c.name AS c\n" +
                                    "FROM sys.schemas s\n" +
                                    "         JOIN sys.tables t ON t.schema_id = s.schema_id\n" +
                                    "         LEFT JOIN sys.indexes i ON i.object_id = t.object_id\n" +
                                    "         LEFT JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id\n" +
                                    "         LEFT JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id\n" +
                                    "WHERE i.type_desc IN ('NONCLUSTERED')\n" +
                                    "  AND i.is_disabled = 0\n" +
                                    "ORDER BY s.name, t.name, i.index_id, ic.key_ordinal")) {
                                try (IndexCollector indexCollector = new IndexCollector((schema, table, name, unique, primaryKey, columns) -> {
                                    TableName tableName = new TableName(catalog, schema, table);
                                    if (datasetTableNames.contains(tableName)) {
                                        Index index = new Index(name, tableName, unique, primaryKey, columns);
                                        indexes.computeIfAbsent(tableName, it -> new ArrayList<>()).add(index);
                                    }
                                })) {
                                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                                        while (resultSet.next()) {
                                            indexCollector.push(
                                                    resultSet.getString("s"),
                                                    resultSet.getString("t"),
                                                    resultSet.getString("i"),
                                                    resultSet.getBoolean("is_unique"),
                                                    resultSet.getBoolean("is_primary_key"),
                                                    resultSet.getString("c")
                                            );
                                        }
                                    }
                                }
                            }
                            // Collects the foreign keys
                            try (PreparedStatement preparedStatement = connection.prepareStatement("\n" +
                                    "SELECT s.name   AS s,\n" +
                                    "       t.name   AS t,\n" +
                                    "       fk.name  AS fk_name,\n" +
                                    "       m_c.name AS col,\n" +
                                    "       r_s.name AS ref_schema,\n" +
                                    "       r_t.name AS ref_table,\n" +
                                    "       o_c.name AS rec_col\n" +
                                    "FROM sys.schemas s\n" +
                                    "         JOIN sys.tables t ON t.schema_id = s.schema_id\n" +
                                    "         JOIN sys.foreign_keys fk ON fk.parent_object_id = t.object_id\n" +
                                    "         JOIN sys.tables r_t ON r_t.object_id = fk.referenced_object_id\n" +
                                    "         JOIN sys.schemas r_s ON r_s.schema_id = r_t.schema_id\n" +
                                    "         JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id\n" +
                                    "         JOIN sys.columns o_c ON o_c.object_id = fkc.referenced_object_id AND o_c.column_id = fkc.referenced_column_id\n" +
                                    "         JOIN sys.columns m_c ON m_c.object_id = fkc.parent_object_id AND m_c.column_id = fkc.parent_column_id\n" +
                                    "ORDER BY fk.name, s.name, t.name, m_c.column_id")) {
                                try (ForeignKeyCollector foreignKeyCollector = new ForeignKeyCollector((constraint, constraintDef, fkSchema, fkTable, fkColumns, pkSchema, pkTable, pkColumns) -> {
                                    TableName pkTableName = new TableName(catalog, pkSchema, pkTable);
                                    TableName fkTableName = new TableName(catalog, fkSchema, fkTable);
                                    if (datasetTableNames.contains(fkTableName)) {
                                        ForeignKey foreignKey = new ForeignKey(
                                                constraint,
                                                null,
                                                pkTableName,
                                                pkColumns,
                                                fkTableName,
                                                fkColumns
                                        );
                                        foreignKeys.computeIfAbsent(fkTableName, tableName -> new ArrayList<>()).add(foreignKey);
                                    }
                                })) {
                                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                                        while (resultSet.next()) {
                                            foreignKeyCollector.push(
                                                    resultSet.getString("fk_name"),
                                                    null,
                                                    resultSet.getString("s"),
                                                    resultSet.getString("t"),
                                                    resultSet.getString("col"),
                                                    resultSet.getString("ref_schema"),
                                                    resultSet.getString("ref_table"),
                                                    resultSet.getString("rec_col")
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tableColumns.entrySet().stream()
                .map(entry -> new Table(
                        entry.getKey(),
                        entry.getValue(),
                        indexes.computeIfAbsent(entry.getKey(), tableName -> Collections.emptyList()),
                        foreignKeys.computeIfAbsent(entry.getKey(), tableName -> Collections.emptyList()))
                )
                .collect(Collectors.toList());
    }

    private static ColumnType getColumnType(String typeName, int typeScale) {
        if ("varchar".equals(typeName)) return ColumnType.VARCHAR;
        if ("nvarchar".equals(typeName)) return ColumnType.VARCHAR;
        if ("int".equals(typeName)) return ColumnType.INTEGER;
        if ("smallint".equals(typeName)) return ColumnType.INTEGER;
        if ("tinyint".equals(typeName)) return ColumnType.INTEGER;
        if ("text".equals(typeName)) return ColumnType.VARCHAR;
        if ("decimal".equals(typeName)) return typeScale > 0 ? ColumnType.BIG_DECIMAL : ColumnType.INTEGER;
        if ("datetime".equals(typeName)) return ColumnType.TIMESTAMP;
        if ("binary".equals(typeName)) return ColumnType.BINARY;
        if ("bit".equals(typeName)) return ColumnType.INTEGER;
        if ("char".equals(typeName)) return ColumnType.VARCHAR;
        if ("image".equals(typeName)) return ColumnType.BINARY;
        if ("varbinary".equals(typeName)) return ColumnType.BINARY;
        throw new RuntimeException("Unexpected type: " + typeName);
    }

    private void identityInsert(TableName tableName, boolean enable) {
        executeSql(
                "SET IDENTITY_INSERT %s %s",
                quote(tableName),
                enable ? "ON" : "OFF"
        );
    }

    @Override
    protected SqlServerDatabaseInserter createInserter(Table table, List<DataFileHeader> dataFileHeaders, String sql) throws SQLException {
        return new SqlServerDatabaseInserter(table, dataFileHeaders, sql);
    }

    @Override
    public DatabasePreparationStrategy<SqlServerDatabase> createDatabasePreparationStrategy(Map<TableName, Table> tablesByName, Set<Table> loadedTables) {
        return SqlServerDisablePreparationStrategy.createPreparationStrategy(this, tablesByName);
    }

    void disableForeignKey(ForeignKey foreignKey) {
        executeSql(
                "ALTER TABLE %s NOCHECK CONSTRAINT %s",
                quote(foreignKey.getFkTableName()),
                foreignKey.getName()
        );
    }

    void enableForeignKey(ForeignKey foreignKey) {
        executeSql(
                "ALTER TABLE %s WITH %s CHECK CONSTRAINT %s",
                quote(foreignKey.getFkTableName()),
                Settings.CHECK_CONTRAINTS ? "CHECK" : "NOCHECK",
                foreignKey.getName()
        );
    }

    class SqlServerDatabaseInserter extends DatabaseInserter {
        private final TableName tableName;
        private final boolean identity;

        SqlServerDatabaseInserter(Table table, List<DataFileHeader> dataFileHeaders, String sql) throws SQLException {
            super(table, dataFileHeaders, sql);

            this.tableName = table.getTableName();
            identity = table.getColumns().stream().anyMatch(Column::isAutoIncrement);
            if (identity) {
                identityInsert(this.tableName, true);
            }
        }

        @Override
        public void close() throws SQLException {
            super.close();

            if (identity) {
                identityInsert(tableName, false);
            }
        }
    }
}
