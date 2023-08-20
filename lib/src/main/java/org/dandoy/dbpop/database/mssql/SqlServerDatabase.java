package org.dandoy.dbpop.database.mssql;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.Settings;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.database.utils.ForeignKeyCollector;
import org.dandoy.dbpop.database.utils.TableCollector;
import org.dandoy.dbpop.upload.DataFileHeader;
import org.dandoy.dbpop.utils.StopWatch;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SqlServerDatabase extends DefaultDatabase {
    private static final Set<String> SYS_SCHEMAS = new HashSet<>(Arrays.asList("guest", "INFORMATION_SCHEMA", "sys", "db_owner", "db_accessadmin", "db_securityadmin", "db_ddladmin", "db_backupoperator", "db_datareader", "db_datawriter", "db_denydatareader", "db_denydatawriter"));
    private static final boolean QUOTE_WITH_BRACKETS = true;
    private final Transitions transitions = new Transitions(this);

    public SqlServerDatabase(ConnectionBuilder connectionBuilder) {
        super(connectionBuilder);
    }

    @Override
    public boolean isSqlServer() {
        return true;
    }

    @Override
    public String quote(String s) {
        if (QUOTE_WITH_BRACKETS) {
            return "[" + s + "]";
        } else {
            return super.quote(s);
        }
    }

    void use(String catalog) {
        try (Statement statement = getConnection().createStatement()) {
            statement.execute("USE " + catalog);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<TableName> getTableNames(String catalog, String schema) {
        try {
            use(catalog);
            try (PreparedStatement preparedStatement = getConnection().prepareStatement("""
                    SELECT s.name AS schema_name, t.name AS table_name
                    FROM sys.schemas s
                             JOIN sys.tables t ON s.schema_id = t.schema_id
                    WHERE s.name = ?
                      AND t.is_ms_shipped = 0
                    """)) {
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

    @Override
    public Collection<Table> getTables() {
        Collection<Table> ret = new ArrayList<>();
        for (String catalog : getCatalogs()) {
            Collection<Table> tables = getTables(catalog);
            ret.addAll(tables);
        }
        return ret;
    }

    static SqlServerColumn toSqlServerColumn(ResultSet resultSet) throws SQLException {
        String column = resultSet.getString("c");
        boolean nullable = resultSet.getBoolean("is_nullable");
        String seedValue = resultSet.getString("seed_value");
        String incrementValue = resultSet.getString("increment_value");
        String typeName = resultSet.getString("type_name");
        Integer typePrecision = resultSet.getObject("type_precision", Integer.class);
        Integer typeMaxLength = resultSet.getObject("type_max_length", Integer.class);
        Integer typeScale = resultSet.getObject("type_scale", Integer.class);
        String defaultConstraintName = resultSet.getString("default_constraint_name");
        String defaultValue = resultSet.getString("default_value");
        try {
            ColumnType columnType = ColumnType.getColumnType(typeName, typePrecision);
            return new SqlServerColumn(
                    column,
                    columnType,
                    nullable,
                    typeName,
                    typePrecision,
                    typeMaxLength,
                    typeScale,
                    seedValue,
                    incrementValue,
                    defaultConstraintName,
                    defaultValue
            );
        } catch (RuntimeException e) {
            // Ignore the unknown column types
            return null;
        }
    }

    @Override
    public SqlServerDatabaseIntrospector createDatabaseIntrospector() {
        return new SqlServerDatabaseIntrospector(this);
    }

    @Override
    public Collection<String> getCatalogs() {
        try (PreparedStatement databasesStatement = getConnection().prepareStatement("""
                SELECT name
                FROM sys.databases
                WHERE state = 0
                  AND name != 'tempdb'
                ORDER BY name""")) {
            try (ResultSet databaseResultSet = databasesStatement.executeQuery()) {
                Collection<String> ret = new ArrayList<>();

                while (databaseResultSet.next()) {
                    String catalog = databaseResultSet.getString("name");
                    ret.add(catalog);
                }
                return ret;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    @SneakyThrows
    public Collection<Table> getTables(String catalog) {
        Map<TableName, List<Column>> tableColumns = new HashMap<>();
        Map<TableName, List<ForeignKey>> foreignKeys = new HashMap<>();
        Map<TableName, List<Index>> indexes = new HashMap<>();
        Map<TableName, SqlServerPrimaryKey> primaryKeyMap = new HashMap<>();
        use(catalog);
        // Collects the tables and columns
        try (PreparedStatement tablesStatement = getConnection().prepareStatement("""
                SELECT s.name             AS s,
                       t.name             AS t,
                       c.name             AS c,
                       c.is_nullable      AS is_nullable,
                       ic.seed_value      AS seed_value,
                       ic.increment_value AS increment_value,
                       ty.name            AS type_name,
                       c.max_length       AS type_max_length,
                       c.precision        AS type_precision,
                       c.scale            AS type_scale,
                       dc.name            AS default_constraint_name,
                       dc.definition      AS default_value
                FROM sys.schemas s
                         JOIN sys.tables t ON t.schema_id = s.schema_id
                         JOIN sys.columns c ON c.object_id = t.object_id
                         LEFT JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                         LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.name = c.name
                         LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
                WHERE t.is_ms_shipped = 0
                ORDER BY s.name, t.name, c.column_id""")) {
            try (TableCollector tableCollector = new TableCollector((schema, table, columns) -> {
                TableName tableName = new TableName(catalog, schema, table);
                tableColumns.put(tableName, columns);
            })) {
                try (ResultSet tablesResultSet = tablesStatement.executeQuery()) {
                    while (tablesResultSet.next()) {
                        SqlServerColumn sqlServerColumn = toSqlServerColumn(tablesResultSet);
                        if (sqlServerColumn != null) {
                            String schema = tablesResultSet.getString("s");
                            String table = tablesResultSet.getString("t");
                            tableCollector.push(schema, table, sqlServerColumn);
                        }
                    }
                }
            }
        }
        // Collects the indexes
        try (PreparedStatement preparedStatement = getConnection().prepareStatement("""
                SELECT s.name AS s,
                       t.name AS t,
                       i.name AS i,
                       i.is_unique,
                       i.is_primary_key,
                       i.type_desc,
                       c.name AS c,
                       ic.is_included_column
                FROM sys.schemas s
                         JOIN sys.tables t ON t.schema_id = s.schema_id
                         LEFT JOIN sys.indexes i ON i.object_id = t.object_id
                         LEFT JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id
                         LEFT JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
                WHERE i.name IS NOT NULL
                  AND c.name IS NOT NULL
                  AND t.is_ms_shipped = 0
                ORDER BY s.name, t.name, i.index_id, ic.key_ordinal""")) {
            try (SqlServerIndexCollector indexCollector = new SqlServerIndexCollector(collector -> {
                TableName tableName = new TableName(catalog, collector.getSchema(), collector.getTable());
                SqlServerIndex index = new SqlServerIndex(collector.getName(), tableName, collector.isUnique(), collector.isPrimaryKey(), collector.getTypeDesc(), collector.getSqlServerIndexColumns());
                indexes.computeIfAbsent(tableName, it2 -> new ArrayList<>()).add(index);
                if (collector.isPrimaryKey()) {
                    primaryKeyMap.put(
                            tableName,
                            new SqlServerPrimaryKey(
                                    collector.getName(),
                                    collector.getTypeDesc(),
                                    collector.getSqlServerIndexColumns()
                            ));
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
                                resultSet.getString("type_desc"),
                                resultSet.getString("c"),
                                resultSet.getBoolean("is_included_column")
                        );
                    }
                }
            }
        }
        // Collects the foreign keys
        try (PreparedStatement preparedStatement = getConnection().prepareStatement("""
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
                WHERE t.is_ms_shipped = 0
                ORDER BY fk.name, s.name, t.name, m_c.column_id""")) {
            try (ForeignKeyCollector foreignKeyCollector = new ForeignKeyCollector((constraint, constraintDef, fkSchema, fkTable, fkColumns, pkSchema, pkTable, pkColumns) -> {
                TableName pkTableName = new TableName(catalog, pkSchema, pkTable);
                TableName fkTableName = new TableName(catalog, fkSchema, fkTable);
                ForeignKey foreignKey = new ForeignKey(
                        constraint,
                        null,
                        pkTableName,
                        pkColumns,
                        fkTableName,
                        fkColumns
                );
                foreignKeys.computeIfAbsent(fkTableName, tableName -> new ArrayList<>()).add(foreignKey);
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
        return tableColumns.entrySet().stream()
                .map(entry -> {
                            TableName tableName = entry.getKey();
                            return new SqlServerTable(
                                    tableName,
                                    entry.getValue(),
                                    indexes.computeIfAbsent(tableName, it -> Collections.emptyList()),
                                    primaryKeyMap.get(tableName),
                                    foreignKeys.computeIfAbsent(tableName, it -> new ArrayList<>()));
                        }
                )
                .collect(Collectors.toList());
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    @SneakyThrows
    public Collection<Table> getTables(Set<TableName> datasetTableNames) {
        Set<String> catalogs = datasetTableNames.stream().map(TableName::getCatalog).collect(Collectors.toSet());
        Map<TableName, List<Column>> tableColumns = new HashMap<>();
        Map<TableName, List<ForeignKey>> foreignKeys = new HashMap<>();
        Map<TableName, List<Index>> indexes = new HashMap<>();
        Map<TableName, SqlServerPrimaryKey> primaryKeyMap = new HashMap<>();
        Connection connection = getConnection();
        try (PreparedStatement databasesStatement = connection.prepareStatement("""
                SELECT name
                FROM sys.databases
                WHERE name NOT IN ('tempdb')
                AND state = 0""")) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet databaseResultSet = databasesStatement.executeQuery()) {
                    while (databaseResultSet.next()) {
                        String catalog = databaseResultSet.getString("name");
                        if (catalogs.contains(catalog)) {
                            statement.execute("USE " + catalog);
                            // Collects the tables and columns
                            try (PreparedStatement tablesStatement = connection.prepareStatement("""
                                    SELECT s.name             AS s,
                                           t.name             AS t,
                                           c.name             AS c,
                                           c.is_nullable      AS is_nullable,
                                           ic.seed_value      AS seed_value,
                                           ic.increment_value AS increment_value,
                                           ty.name            AS type_name,
                                           c.max_length       AS type_max_length,
                                           c.precision        AS type_precision,
                                           c.scale            AS type_scale,
                                           dc.name            AS default_constraint_name,
                                           dc.definition      AS default_value
                                    FROM sys.schemas s
                                             JOIN sys.tables t ON t.schema_id = s.schema_id
                                             JOIN sys.columns c ON c.object_id = t.object_id
                                             LEFT JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                                             LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.name = c.name
                                             LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
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
                                                SqlServerColumn sqlServerColumn = toSqlServerColumn(tablesResultSet);
                                                if (sqlServerColumn != null) {
                                                    tableCollector.push(schema, table, sqlServerColumn);
                                                }
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
                                           i.type_desc,
                                           c.name AS c,
                                           ic.is_included_column
                                    FROM sys.schemas s
                                             JOIN sys.tables t ON t.schema_id = s.schema_id
                                             LEFT JOIN sys.indexes i ON i.object_id = t.object_id
                                             LEFT JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id
                                             LEFT JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
                                    WHERE i.name IS NOT NULL
                                      AND c.name IS NOT NULL
                                    ORDER BY s.name, t.name, i.index_id, ic.key_ordinal""")) {
                                try (SqlServerIndexCollector indexCollector = new SqlServerIndexCollector(collector -> {
                                    TableName tableName = new TableName(catalog, collector.getSchema(), collector.getTable());
                                    if (datasetTableNames.contains(tableName)) {
                                        SqlServerIndex index = new SqlServerIndex(collector.getName(), tableName, collector.isUnique(), collector.isPrimaryKey(), collector.getTypeDesc(), collector.getSqlServerIndexColumns());
                                        indexes.computeIfAbsent(tableName, it2 -> new ArrayList<>()).add(index);
                                        if (collector.isPrimaryKey()) {
                                            primaryKeyMap.put(
                                                    tableName,
                                                    new SqlServerPrimaryKey(
                                                            collector.getName(),
                                                            collector.getTypeDesc(),
                                                            collector.getSqlServerIndexColumns()
                                                    )
                                            );
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
                                                    resultSet.getString("type_desc"),
                                                    resultSet.getString("c"),
                                                    resultSet.getBoolean("is_included_column")
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
                            return new SqlServerTable(
                                    tableName,
                                    entry.getValue(),
                                    indexes.computeIfAbsent(tableName, it -> Collections.emptyList()),
                                    primaryKeyMap.get(tableName),
                                    foreignKeys.computeIfAbsent(tableName, it -> new ArrayList<>()));
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
        List<SqlServerPrimaryKey> primaryKeys = new ArrayList<>();
        try {
            Connection connection = getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.execute("USE " + tableName.getCatalog());
            }
            // Collects the tables and columns
            try (PreparedStatement tablesStatement = connection.prepareStatement("""
                    SELECT s.name             AS s,
                           t.name             AS t,
                           c.name             AS c,
                           c.is_nullable      AS is_nullable,
                           ic.seed_value      AS seed_value,
                           ic.increment_value AS increment_value,
                           ty.name            AS type_name,
                           c.max_length       AS type_max_length,
                           c.precision        AS type_precision,
                           c.scale            AS type_scale,
                           dc.name            AS default_constraint_name,
                           dc.definition      AS default_value
                    FROM sys.schemas s
                             JOIN sys.tables t ON t.schema_id = s.schema_id
                             JOIN sys.columns c ON c.object_id = t.object_id
                             LEFT JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                             LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.name = c.name
                             LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
                    WHERE s.name = ?
                      AND t.name = ?
                    ORDER BY s.name, t.name, c.column_id""")) {
                tablesStatement.setString(1, tableName.getSchema());
                tablesStatement.setString(2, tableName.getTable());
                try (ResultSet tablesResultSet = tablesStatement.executeQuery()) {
                    while (tablesResultSet.next()) {
                        SqlServerColumn sqlServerColumn = toSqlServerColumn(tablesResultSet);
                        if (sqlServerColumn != null) {
                            tableColumns.add(sqlServerColumn);
                        }
                    }
                }
            }
            // Collects the indexes
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT i.name AS i,
                           i.is_unique,
                           i.is_primary_key,
                           i.type_desc,
                           c.name AS c,
                           ic.is_included_column
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
                try (SqlServerIndexCollector indexCollector = new SqlServerIndexCollector(collector -> {
                    SqlServerIndex index = new SqlServerIndex(collector.getName(), tableName, collector.isUnique(), collector.isPrimaryKey(), collector.getTypeDesc(), collector.getSqlServerIndexColumns());
                    indexes.add(index);
                    if (collector.isPrimaryKey()) {
                        primaryKeys.add(
                                new SqlServerPrimaryKey(
                                        collector.getName(),
                                        collector.getTypeDesc(),
                                        collector.getSqlServerIndexColumns()
                                )
                        );
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
                                    resultSet.getString("type_desc"),
                                    resultSet.getString("c"),
                                    resultSet.getBoolean("is_included_column")
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
        if (tableColumns.isEmpty()) {
            return null;
        }
        return new SqlServerTable(
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
            try (PreparedStatement preparedStatement = getConnection().prepareStatement("""
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
        try (PreparedStatement preparedStatement = getConnection().prepareStatement(sql)) {
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

    private void setNextIdentityValue(TableName tableName, long value) {
        executeSql(
                "DBCC CHECKIDENT ('%s', RESEED, %d);\n",
                quote(tableName),
                value
        );
    }

    @Override
    protected SqlServerDatabaseInserter createInserter(Table table, List<DataFileHeader> dataFileHeaders, String sql) throws SQLException {
        return new SqlServerDatabaseInserter(table, dataFileHeaders, sql);
    }

    @Override
    public String getTableDefinition(TableName tableName) {
        return super.getTableDefinition(tableName);
    }

    @Override
    public DatabasePreparationFactory createDatabasePreparationFactory() {
        return DisableForeignKeysPreparationStrategy::new;
    }

    @Override
    public RowCount getRowCount(TableName tableName) {
        return getRowCount("SELECT TOP (%d) 1 FROM %s".formatted(ROW_COUNT_MAX + 1, quote(tableName)));
    }

    @Override
    public void disableForeignKey(ForeignKey foreignKey) {
        StopWatch.record("disableForeignKey", () -> executeSql(
                "ALTER TABLE %s NOCHECK CONSTRAINT %s",
                quote(foreignKey.getFkTableName()),
                quote(foreignKey.getName())
        ));
    }

    @Override
    public TransitionGenerator getTransitionGenerator(String objectType) {
        return switch (objectType) {
            case "USER_TABLE" -> transitions.tableTransitionGenerator;
            case "PRIMARY_KEY" -> transitions.primaryKeyTransitionGenerator;
            case "INDEX" -> transitions.indexTransitionGenerator;
            case "FOREIGN_KEY_CONSTRAINT" -> transitions.foreignKeyTransitionGenerator;
            case "SQL_INLINE_TABLE_VALUED_FUNCTION",
                    "SQL_SCALAR_FUNCTION",
                    "SQL_STORED_PROCEDURE",
                    "SQL_TABLE_VALUED_FUNCTION",
                    "SQL_TRIGGER",
                    "VIEW" -> transitions.changeCreateToAlterTransitionGenerator;
            default -> super.getTransitionGenerator(objectType);
        };
    }

    @Override
    public void createCatalog(String catalog) {
        String sql = String.format("CREATE DATABASE %s", catalog);
        try {
            log.debug("SQL: {}", sql);
            try (Statement statement = getConnection().createStatement()) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            if ("S0003".equals(e.getSQLState())) return;
            throw new RuntimeException(String.format("Failed to execute \"%s\"", sql), e);
        }
    }

    @Override
    public void createShema(String catalog, String schema) {
        String sql = "CREATE SCHEMA %s".formatted(quote(schema));
        try {
            log.debug("SQL: {}", sql);
            try (Statement statement = getConnection().createStatement()) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            if ("S0001".equals(e.getSQLState())) return;
            throw new RuntimeException(String.format("Failed to execute \"%s\"", sql), e);
        }
    }

    @Override
    public void dropObject(ObjectIdentifier objectIdentifier) {
        String sql = getTransitionGenerator(objectIdentifier.getType()).drop(objectIdentifier);
        // use(objectIdentifier.getCatalog());
        executeSql(sql);
    }

    @Override
    public void enableForeignKey(ForeignKey foreignKey) {
        StopWatch.record("enableForeignKey", () -> executeSql(
                "ALTER TABLE %s WITH %s CHECK CONSTRAINT %s",
                quote(foreignKey.getFkTableName()),
                Settings.CHECK_CONTRAINTS ? "CHECK" : "NOCHECK",
                quote(foreignKey.getName())
        ));
    }

    class SqlServerDatabaseInserter extends DatabaseInserter {
        private final TableName tableName;
        private final boolean identity;
        private IdentityColumnInserter identityColumnInserter;

        SqlServerDatabaseInserter(Table table, List<DataFileHeader> dataFileHeaders, String sql) throws SQLException {
            super(table, dataFileHeaders, sql);

            this.tableName = table.getTableName();
            identity = table.getColumns().stream().anyMatch(Column::isAutoIncrement);
            if (identity) {
                identityInsert(this.tableName, true);
            }
        }

        @Override
        protected void addColumnInserter(DataFileHeader dataFileHeader, int csvPos, int jdbcPos, Column column) {
            super.addColumnInserter(dataFileHeader, csvPos, jdbcPos, column);

            if (column.isAutoIncrement()) {
                ColumnType columnType = column.getColumnType();
                if (columnType == ColumnType.INTEGER) {
                    identityColumnInserter = new IdentityColumnInserter(csvPos, jdbcPos, columnType);
                    addColumnInserter(identityColumnInserter);
                }
            }
        }

        private class IdentityColumnInserter extends ColumnInserter {
            private Long max;

            public IdentityColumnInserter(int csvPos, int jdbcPos, ColumnType columnType) {
                super(csvPos, jdbcPos, columnType);
            }

            @Override
            public void consume(String s) {
                if (s == null) return; // An identity column with a null value?
                long value = toLong(s);
                if (max == null || value > max) {
                    max = value;
                }
            }
        }

        private static long toLong(String s) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws SQLException {
            try {
                super.close();
            } finally {
                if (identity) {
                    identityInsert(tableName, false);
                    if (identityColumnInserter != null) {
                        if (identityColumnInserter.max != null) {
                            setNextIdentityValue(tableName, identityColumnInserter.max);
                        }
                    }
                }
            }
        }
    }
}
