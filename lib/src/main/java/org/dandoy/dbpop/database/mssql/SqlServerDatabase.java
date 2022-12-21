package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.Settings;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.database.utils.ForeignKeyCollector;
import org.dandoy.dbpop.database.utils.IndexCollector;
import org.dandoy.dbpop.database.utils.TableCollector;
import org.dandoy.dbpop.upload.DataFileHeader;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpop.utils.StopWatch;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class SqlServerDatabase extends Database {
    private static final Set<String> SYS_SCHEMAS = new HashSet<>(Arrays.asList("guest", "INFORMATION_SCHEMA", "sys", "db_owner", "db_accessadmin", "db_securityadmin", "db_ddladmin", "db_backupoperator", "db_datareader", "db_datawriter", "db_denydatareader", "db_denydatawriter"));

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
        Map<TableName, PrimaryKey> primaryKeyMap = new HashMap<>();
        try (PreparedStatement databasesStatement = connection.prepareStatement("SELECT name FROM sys.databases WHERE name NOT IN ('tempdb')")) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet databaseResultSet = databasesStatement.executeQuery()) {
                    while (databaseResultSet.next()) {
                        String catalog = databaseResultSet.getString("name");
                        if (catalogs.contains(catalog)) {
                            statement.execute("USE " + catalog);
                            // Collects the tables and columns
                            try (PreparedStatement tablesStatement = connection.prepareStatement("""
                                    SELECT s.name       AS s,
                                           t.name       AS t,
                                           c.name       AS c,
                                           c.is_nullable,
                                           c.is_identity,
                                           ty.name      AS type_name,
                                           ty.precision AS type_precision
                                    FROM sys.schemas s
                                             JOIN sys.tables t ON t.schema_id = s.schema_id
                                             JOIN sys.columns c ON c.object_id = t.object_id
                                             LEFT JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                                    ORDER BY s.name, t.name, c.column_id""")) {
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
                                                int typePrecision = tablesResultSet.getInt("type_precision");
                                                ColumnType columnType = ColumnType.getColumnType(typeName, typePrecision);
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
                            try (PreparedStatement preparedStatement = connection.prepareStatement("""

                                    SELECT s.name AS s,
                                           t.name AS t,
                                           i.name AS i,
                                           i.is_unique,
                                           i.is_primary_key,
                                           c.name AS c
                                    FROM sys.schemas s
                                             JOIN sys.tables t ON t.schema_id = s.schema_id
                                             LEFT JOIN sys.indexes i ON i.object_id = t.object_id
                                             LEFT JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id
                                             LEFT JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
                                    WHERE i.name IS NOT NULL
                                      AND c.name IS NOT NULL
                                    ORDER BY s.name, t.name, i.index_id, ic.key_ordinal""")) {
                                try (IndexCollector indexCollector = new IndexCollector((schema, table, name, unique, primaryKey, columns) -> {
                                    TableName tableName = new TableName(catalog, schema, table);
                                    if (datasetTableNames.contains(tableName)) {
                                        Index index = new Index(name, tableName, unique, primaryKey, columns);
                                        indexes.computeIfAbsent(tableName, it -> new ArrayList<>()).add(index);
                                        if (primaryKey) {
                                            primaryKeyMap.put(tableName, new PrimaryKey(name, columns));
                                        }
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
                            try (PreparedStatement preparedStatement = connection.prepareStatement("""

                                    SELECT s.name   AS s,
                                           t.name   AS t,
                                           fk.name  AS fk_name,
                                           m_c.name AS col,
                                           r_s.name AS ref_schema,
                                           r_t.name AS ref_table,
                                           o_c.name AS rec_col
                                    FROM sys.schemas s
                                             JOIN sys.tables t ON t.schema_id = s.schema_id
                                             JOIN sys.foreign_keys fk ON fk.parent_object_id = t.object_id
                                             JOIN sys.tables r_t ON r_t.object_id = fk.referenced_object_id
                                             JOIN sys.schemas r_s ON r_s.schema_id = r_t.schema_id
                                             JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                                             JOIN sys.columns o_c ON o_c.object_id = fkc.referenced_object_id AND o_c.column_id = fkc.referenced_column_id
                                             JOIN sys.columns m_c ON m_c.object_id = fkc.parent_object_id AND m_c.column_id = fkc.parent_column_id
                                    ORDER BY fk.name, s.name, t.name, m_c.column_id""")) {
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
                .map(entry -> {
                            TableName tableName = entry.getKey();
                            return new Table(
                                    tableName,
                                    entry.getValue(),
                                    indexes.computeIfAbsent(tableName, it -> Collections.emptyList()),
                                    primaryKeyMap.get(tableName),
                                    foreignKeys.computeIfAbsent(tableName, it -> Collections.emptyList()));
                        }
                )
                .collect(Collectors.toList());
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public Table getTable(TableName tableName) {
        List<Column> tableColumns = new ArrayList<>();
        List<ForeignKey> foreignKeys = new ArrayList<>();
        List<Index> indexes = new ArrayList<>();
        List<PrimaryKey> primaryKeys = new ArrayList<>();
        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute("USE " + tableName.getCatalog());
            }
            // Collects the tables and columns
            try (PreparedStatement tablesStatement = connection.prepareStatement("""
                    SELECT c.name       AS c,
                           c.is_nullable,
                           c.is_identity,
                           ty.name      AS type_name,
                           ty.precision AS type_precision
                    FROM sys.schemas s
                             JOIN sys.tables t ON t.schema_id = s.schema_id
                             JOIN sys.columns c ON c.object_id = t.object_id
                             LEFT JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                    WHERE s.name = ?
                      AND t.name = ?
                    ORDER BY s.name, t.name, c.column_id""")) {
                tablesStatement.setString(1, tableName.getSchema());
                tablesStatement.setString(2, tableName.getTable());
                try (ResultSet tablesResultSet = tablesStatement.executeQuery()) {
                    while (tablesResultSet.next()) {
                        String column = tablesResultSet.getString("c");
                        boolean nullable = tablesResultSet.getBoolean("is_nullable");
                        boolean identity = tablesResultSet.getBoolean("is_identity");
                        String typeName = tablesResultSet.getString("type_name");
                        int typePrecision = tablesResultSet.getInt("type_precision");
                        ColumnType columnType = ColumnType.getColumnType(typeName, typePrecision);
                        tableColumns.add(new Column(column, columnType, nullable, identity));
                    }
                }
            }
            // Collects the indexes
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT i.name AS i,
                           i.is_unique,
                           i.is_primary_key,
                           c.name AS c
                    FROM sys.schemas s
                             JOIN sys.tables t ON t.schema_id = s.schema_id
                             LEFT JOIN sys.indexes i ON i.object_id = t.object_id
                             LEFT JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id
                             LEFT JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
                    WHERE i.name IS NOT NULL
                      AND c.name IS NOT NULL
                      AND s.name = ?
                      AND t.name = ?
                    ORDER BY s.name, t.name, i.index_id, ic.key_ordinal""")) {
                preparedStatement.setString(1, tableName.getSchema());
                preparedStatement.setString(2, tableName.getTable());
                try (IndexCollector indexCollector = new IndexCollector((schema, table, name, unique, primaryKey, columns) -> {
                    Index index = new Index(name, tableName, unique, primaryKey, columns);
                    indexes.add(index);
                    if (primaryKey) {
                        primaryKeys.add(new PrimaryKey(name, columns));
                    }
                })) {
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            indexCollector.push(
                                    tableName.getSchema(),
                                    tableName.getTable(),
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
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT fk.name  AS fk_name,
                           m_c.name AS col,
                           r_s.name AS ref_schema,
                           r_t.name AS ref_table,
                           o_c.name AS rec_col
                    FROM sys.schemas s
                             JOIN sys.tables t ON t.schema_id = s.schema_id
                             JOIN sys.foreign_keys fk ON fk.parent_object_id = t.object_id
                             JOIN sys.tables r_t ON r_t.object_id = fk.referenced_object_id
                             JOIN sys.schemas r_s ON r_s.schema_id = r_t.schema_id
                             JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                             JOIN sys.columns o_c ON o_c.object_id = fkc.referenced_object_id AND o_c.column_id = fkc.referenced_column_id
                             JOIN sys.columns m_c ON m_c.object_id = fkc.parent_object_id AND m_c.column_id = fkc.parent_column_id
                    WHERE s.name = ?
                      AND t.name = ?
                    ORDER BY fk.name, s.name, t.name, m_c.column_id""")) {
                preparedStatement.setString(1, tableName.getSchema());
                preparedStatement.setString(2, tableName.getTable());
                try (ForeignKeyCollector foreignKeyCollector = new ForeignKeyCollector((constraint, constraintDef, fkSchema, fkTable, fkColumns, pkSchema, pkTable, pkColumns) -> {
                    TableName pkTableName = new TableName(tableName.getCatalog(), pkSchema, pkTable);
                    ForeignKey foreignKey = new ForeignKey(constraint, null, pkTableName, pkColumns, tableName, fkColumns);
                    foreignKeys.add(foreignKey);
                })) {
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            foreignKeyCollector.push(
                                    resultSet.getString("fk_name"),
                                    null,
                                    tableName.getSchema(),
                                    tableName.getTable(),
                                    resultSet.getString("col"),
                                    resultSet.getString("ref_schema"),
                                    resultSet.getString("ref_table"),
                                    resultSet.getString("rec_col")
                            );
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new Table(
                tableName,
                tableColumns,
                indexes,
                primaryKeys.isEmpty() ? null : primaryKeys.get(0),
                foreignKeys
        );
    }

    @Override
    public List<ForeignKey> getRelatedForeignKeys(TableName tableName) {
        try {
            List<ForeignKey> foreignKeys = new ArrayList<>();
            use(tableName.getCatalog());
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT s.name   AS s,
                           t.name   AS t,
                           fk.name  AS fk_name,
                           m_c.name AS col,
                           o_c.name AS rec_col
                    FROM sys.schemas s
                             JOIN sys.tables t ON t.schema_id = s.schema_id
                             JOIN sys.foreign_keys fk ON fk.parent_object_id = t.object_id
                             JOIN sys.tables r_t ON r_t.object_id = fk.referenced_object_id
                             JOIN sys.schemas r_s ON r_s.schema_id = r_t.schema_id
                             JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                             JOIN sys.columns o_c ON o_c.object_id = fkc.referenced_object_id AND o_c.column_id = fkc.referenced_column_id
                             JOIN sys.columns m_c ON m_c.object_id = fkc.parent_object_id AND m_c.column_id = fkc.parent_column_id
                    WHERE r_s.name = ?
                      AND r_t.name = ?
                    ORDER BY fk.name, s.name, t.name, m_c.column_id
                    """)) {
                preparedStatement.setString(1, tableName.getSchema());
                preparedStatement.setString(2, tableName.getTable());
                try (ForeignKeyCollector foreignKeyCollector = new ForeignKeyCollector((constraint, constraintDef, fkSchema, fkTable, fkColumns, pkSchema, pkTable, pkColumns) -> {
                    TableName pkTableName = new TableName(tableName.getCatalog(), pkSchema, pkTable);
                    TableName fkTableName = new TableName(tableName.getCatalog(), fkSchema, fkTable);
                    foreignKeys.add(new ForeignKey(
                            constraint,
                            null,
                            pkTableName,
                            pkColumns,
                            fkTableName,
                            fkColumns
                    ));
                })) {
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            foreignKeyCollector.push(
                                    resultSet.getString("fk_name"),
                                    null,
                                    resultSet.getString("s"),
                                    resultSet.getString("t"),
                                    resultSet.getString("col"),
                                    tableName.getSchema(),
                                    tableName.getTable(),
                                    resultSet.getString("rec_col")
                            );
                        }
                    }
                }
            }
            return foreignKeys;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getSchemas(String catalog) {
        String sql = String.format("SELECT name FROM %s.sys.schemas", quote(catalog));
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                List<String> ret = new ArrayList<>();
                while (resultSet.next()) {
                    String name = resultSet.getString(1);
                    if (!SYS_SCHEMAS.contains(name)) {
                        ret.add(name);
                    }
                }
                return ret;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
    public DatabasePreparationStrategy createDatabasePreparationStrategy(Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, List<String> datasets) {
        return SqlServerDisablePreparationStrategy.createPreparationStrategy(this, datasetsByName, tablesByName, datasets);
    }

    void disableForeignKey(ForeignKey foreignKey) {
        StopWatch.record("disableForeignKey", () -> executeSql(
                "ALTER TABLE %s NOCHECK CONSTRAINT %s",
                quote(foreignKey.getFkTableName()),
                foreignKey.getName()
        ));
    }

    void enableForeignKey(ForeignKey foreignKey) {
        StopWatch.record("enableForeignKey", () -> executeSql(
                "ALTER TABLE %s WITH %s CHECK CONSTRAINT %s",
                quote(foreignKey.getFkTableName()),
                Settings.CHECK_CONTRAINTS ? "CHECK" : "NOCHECK",
                foreignKey.getName()
        ));
    }

    class SqlServerDatabaseInserter extends DatabaseInserter {
        private final TableName tableName;
        private final boolean identity;

        SqlServerDatabaseInserter(Table table, List<DataFileHeader> dataFileHeaders, String sql) throws SQLException {
            super(table, dataFileHeaders, sql);

            this.tableName = table.tableName();
            identity = table.columns().stream().anyMatch(Column::isAutoIncrement);
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
