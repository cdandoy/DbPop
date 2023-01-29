package org.dandoy.dbpop.database.pgsql;

import lombok.SneakyThrows;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.database.utils.ForeignKeyCollector;
import org.dandoy.dbpop.database.utils.IndexCollector;
import org.dandoy.dbpop.database.utils.TableCollector;
import org.dandoy.dbpop.utils.NotImplementedException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class PostgresDatabase extends DefaultDatabase {

    public PostgresDatabase(ConnectionBuilder connection) {
        super(connection);
    }

    @Override
    public DatabaseIntrospector createDatabaseIntrospector() {
        throw new NotImplementedException();
    }

    private void checkCatalog(String catalog) {
        try {
            if (catalog != null && !catalog.equals(getConnection().getCatalog())) {
                throw new RuntimeException(String.format(
                        "Cannot reach database %s when connected to database %s",
                        catalog, getConnection().getCatalog()
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<TableName> getTableNames(String catalog, String schema) {
        try {
            checkCatalog(catalog);

            try (PreparedStatement preparedStatement = getConnection().prepareStatement("""
                    SELECT table_catalog,
                           table_schema,
                           table_name
                    FROM information_schema.tables t
                    WHERE table_type = 'BASE TABLE'
                      AND table_schema = ?""")) {
                preparedStatement.setString(1, schema);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    Collection<TableName> ret = new ArrayList<>();
                    while (resultSet.next()) {
                        String tableCatalog = resultSet.getString("table_catalog");
                        String tableSchema = resultSet.getString("table_schema");
                        String tableName = resultSet.getString("table_name");
                        ret.add(new TableName(tableCatalog, tableSchema, tableName));
                    }
                    return ret;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    @SneakyThrows
    public Collection<Table> getTables() {
        String catalog = connection.getCatalog();
        return getTables(catalog);
    }

    @Override
    @SneakyThrows
    public Collection<String> getCatalogs() {
        return Collections.singleton(connection.getCatalog());
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public Collection<Table> getTables(String catalog) {
        try {
            Map<TableName, List<Column>> tableColumns = new HashMap<>();
            Map<TableName, List<ForeignKey>> foreignKeys = new HashMap<>();
            Map<TableName, List<Index>> indexes = new HashMap<>();
            Map<TableName, PrimaryKey> primaryKeyMap = new HashMap<>();
            Connection connection = getConnection();
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT table_schema,
                           table_name,
                           column_name,
                           is_identity,
                           c.data_type,
                           c.numeric_scale,
                           c.is_nullable
                    FROM information_schema.columns c
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                    ORDER BY table_catalog, table_schema, table_name, ordinal_position""")) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    try (TableCollector tableCollector = new TableCollector((schema, table, columns) -> {
                        TableName tableName = new TableName(catalog, schema, table);
                        tableColumns.put(tableName, columns);
                    })) {
                        while (resultSet.next()) {
                            String tableSchema = resultSet.getString("table_schema");
                            String tableName = resultSet.getString("table_name");
                            String columnName = resultSet.getString("column_name");
                            boolean isIdentity = "YES".equals(resultSet.getString("is_identity"));
                            String dataType = resultSet.getString("data_type");
                            int numericScale = resultSet.getInt("numeric_scale");
                            boolean nullable = "YES".equals(resultSet.getString("is_nullable"));
                            ColumnType columnType = getColumnType(dataType, numericScale);
                            tableCollector.push(
                                    tableSchema,
                                    tableName,
                                    new Column(
                                            columnName,
                                            columnType,
                                            nullable,
                                            isIdentity
                                    )
                            );
                        }
                    }
                }
            }

            // Collects the indexes
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT s.nspname       AS schema_name,
                           t.relname       AS table_name,
                           i.relname       AS index_name,
                           c.attname       AS column_name,
                           ix.indisunique  AS is_unique,
                           ix.indisprimary AS is_primary_key
                    FROM pg_catalog.pg_class t
                             JOIN pg_catalog.pg_namespace s ON s.oid = t.relnamespace
                             JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
                             JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
                             JOIN pg_catalog.pg_attribute c ON c.attrelid = t.oid
                    WHERE c.attnum = ANY (ix.indkey)
                      AND t.relkind = 'r'
                    ORDER BY t.relname, i.relname, c.attnum""")) {
                try (IndexCollector indexCollector = new IndexCollector(it -> {
                    TableName tableName = new TableName(catalog, it.getSchema(), it.getTable());
                    Index index = new Index(it.getName(), tableName, it.isUnique(), it.isPrimaryKey(), it.getColumns());
                    indexes.computeIfAbsent(tableName, it2 -> new ArrayList<>()).add(index);
                    if (it.isPrimaryKey()) {
                        primaryKeyMap.put(tableName, new PrimaryKey(it.getName(), it.getColumns()));
                    }
                })) {
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            indexCollector.push(
                                    resultSet.getString("schema_name"),
                                    resultSet.getString("table_name"),
                                    resultSet.getString("index_name"),
                                    resultSet.getBoolean("is_unique"),
                                    resultSet.getBoolean("is_primary_key"),
                                    resultSet.getString("column_name")
                            );
                        }
                    }
                }
            }
            // Collects the foreign keys
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    WITH unnested_confkey AS (SELECT oid, UNNEST(confkey) AS confkey
                                              FROM pg_constraint),
                         unnested_conkey AS (SELECT oid, UNNEST(conkey) AS conkey
                                             FROM pg_constraint)
                    SELECT c.conname                      AS constraint_name,
                           s.nspname                      AS constraint_schema,
                           t.relname                      AS constraint_table,
                           col.attname                    AS constraint_column,
                           PG_GET_CONSTRAINTDEF(conf.oid) AS constraint_def,
                           rs.nspname                     AS referenced_schema,
                           rt.relname                     AS referenced_table,
                           rf.attname                     AS referenced_column
                    FROM pg_constraint c
                             JOIN unnested_conkey con ON c.oid = con.oid
                             JOIN pg_class t ON t.oid = c.conrelid
                             JOIN pg_catalog.pg_namespace s ON s.oid = t.relnamespace
                             JOIN pg_attribute col ON (col.attrelid = t.oid AND col.attnum = con.conkey)
                             JOIN pg_class rt ON c.confrelid = rt.oid
                             JOIN pg_catalog.pg_namespace rs ON rs.oid = rt.relnamespace
                             JOIN unnested_confkey conf ON c.oid = conf.oid
                             JOIN pg_attribute rf ON (rf.attrelid = c.confrelid AND rf.attnum = conf.confkey)
                    WHERE c.contype = 'f'
                    ORDER BY c.conname, s.nspname, t.relname""")) {
                try (ForeignKeyCollector foreignKeyCollector = new ForeignKeyCollector((constraint, constraintDef, fkSchema, fkTable, fkColumns, pkSchema, pkTable, pkColumns) -> {
                    TableName pkTableName = new TableName(catalog, pkSchema, pkTable);
                    TableName fkTableName = new TableName(catalog, fkSchema, fkTable);
                    ForeignKey foreignKey = new ForeignKey(
                            constraint,
                            constraintDef,
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
                                    resultSet.getString("constraint_name"),
                                    resultSet.getString("constraint_def"),
                                    resultSet.getString("constraint_schema"),
                                    resultSet.getString("constraint_table"),
                                    resultSet.getString("constraint_column"),
                                    resultSet.getString("referenced_schema"),
                                    resultSet.getString("referenced_table"),
                                    resultSet.getString("referenced_column")
                            );
                        }
                    }
                }
            }

            return tableColumns.entrySet().stream()
                    .map(entry -> {
                                TableName tableName = entry.getKey();
                                return new Table(
                                        tableName,
                                        entry.getValue(),
                                        indexes.computeIfAbsent(tableName, it -> Collections.emptyList()),
                                        primaryKeyMap.get(tableName),
                                        foreignKeys.computeIfAbsent(tableName, it -> new ArrayList<>()));
                            }
                    )
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public Collection<Table> getTables(Set<TableName> datasetTableNames) {
        datasetTableNames.stream().map(TableName::getCatalog).forEach(this::checkCatalog);

        try {
            Map<TableName, List<Column>> tableColumns = new HashMap<>();
            Map<TableName, List<ForeignKey>> foreignKeys = new HashMap<>();
            Map<TableName, List<Index>> indexes = new HashMap<>();
            Map<TableName, PrimaryKey> primaryKeyMap = new HashMap<>();
            Connection connection = getConnection();
            String catalog = connection.getCatalog();
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT table_schema,
                           table_name,
                           column_name,
                           is_identity,
                           c.data_type,
                           c.numeric_scale,
                           c.is_nullable
                    FROM information_schema.columns c
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                    ORDER BY table_catalog, table_schema, table_name, ordinal_position""")) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    try (TableCollector tableCollector = new TableCollector((schema, table, columns) -> {
                        TableName tableName = new TableName(catalog, schema, table);
                        if (datasetTableNames.contains(tableName)) {
                            tableColumns.put(tableName, columns);
                        }
                    })) {
                        while (resultSet.next()) {
                            String tableSchema = resultSet.getString("table_schema");
                            String tableName = resultSet.getString("table_name");
                            String columnName = resultSet.getString("column_name");
                            boolean isIdentity = "YES".equals(resultSet.getString("is_identity"));
                            String dataType = resultSet.getString("data_type");
                            int numericScale = resultSet.getInt("numeric_scale");
                            boolean nullable = "YES".equals(resultSet.getString("is_nullable"));
                            ColumnType columnType = getColumnType(dataType, numericScale);
                            tableCollector.push(
                                    tableSchema,
                                    tableName,
                                    new Column(
                                            columnName,
                                            columnType,
                                            nullable,
                                            isIdentity
                                    )
                            );
                        }
                    }
                }
            }

            // Collects the indexes
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT s.nspname       AS schema_name,
                           t.relname       AS table_name,
                           i.relname       AS index_name,
                           c.attname       AS column_name,
                           ix.indisunique  AS is_unique,
                           ix.indisprimary AS is_primary_key
                    FROM pg_catalog.pg_class t
                             JOIN pg_catalog.pg_namespace s ON s.oid = t.relnamespace
                             JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
                             JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
                             JOIN pg_catalog.pg_attribute c ON c.attrelid = t.oid
                    WHERE c.attnum = ANY (ix.indkey)
                      AND t.relkind = 'r'
                    ORDER BY t.relname, i.relname, c.attnum""")) {
                try (IndexCollector indexCollector = new IndexCollector(collector -> {
                    TableName tableName = new TableName(catalog, collector.getSchema(), collector.getTable());
                    if (datasetTableNames.contains(tableName)) {
                        Index index = new Index(collector.getName(), tableName, collector.isUnique(), collector.isPrimaryKey(), collector.getColumns());
                        indexes.computeIfAbsent(tableName, it -> new ArrayList<>()).add(index);
                        if (collector.isPrimaryKey()) {
                            primaryKeyMap.put(tableName, new PrimaryKey(collector.getName(), collector.getColumns()));
                        }
                    }
                })) {
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            indexCollector.push(
                                    resultSet.getString("schema_name"),
                                    resultSet.getString("table_name"),
                                    resultSet.getString("index_name"),
                                    resultSet.getBoolean("is_unique"),
                                    resultSet.getBoolean("is_primary_key"),
                                    resultSet.getString("column_name")
                            );
                        }
                    }
                }
            }
            // Collects the foreign keys
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    WITH unnested_confkey AS (SELECT oid, UNNEST(confkey) AS confkey
                                              FROM pg_constraint),
                         unnested_conkey AS (SELECT oid, UNNEST(conkey) AS conkey
                                             FROM pg_constraint)
                    SELECT c.conname                      AS constraint_name,
                           s.nspname                      AS constraint_schema,
                           t.relname                      AS constraint_table,
                           col.attname                    AS constraint_column,
                           PG_GET_CONSTRAINTDEF(conf.oid) AS constraint_def,
                           rs.nspname                     AS referenced_schema,
                           rt.relname                     AS referenced_table,
                           rf.attname                     AS referenced_column
                    FROM pg_constraint c
                             JOIN unnested_conkey con ON c.oid = con.oid
                             JOIN pg_class t ON t.oid = c.conrelid
                             JOIN pg_catalog.pg_namespace s ON s.oid = t.relnamespace
                             JOIN pg_attribute col ON (col.attrelid = t.oid AND col.attnum = con.conkey)
                             JOIN pg_class rt ON c.confrelid = rt.oid
                             JOIN pg_catalog.pg_namespace rs ON rs.oid = rt.relnamespace
                             JOIN unnested_confkey conf ON c.oid = conf.oid
                             JOIN pg_attribute rf ON (rf.attrelid = c.confrelid AND rf.attnum = conf.confkey)
                    WHERE c.contype = 'f'
                    ORDER BY c.conname, s.nspname, t.relname""")) {
                try (ForeignKeyCollector foreignKeyCollector = new ForeignKeyCollector((constraint, constraintDef, fkSchema, fkTable, fkColumns, pkSchema, pkTable, pkColumns) -> {
                    TableName pkTableName = new TableName(catalog, pkSchema, pkTable);
                    TableName fkTableName = new TableName(catalog, fkSchema, fkTable);
                    if (datasetTableNames.contains(fkTableName)) {
                        ForeignKey foreignKey = new ForeignKey(
                                constraint,
                                constraintDef,
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
                                    resultSet.getString("constraint_name"),
                                    resultSet.getString("constraint_def"),
                                    resultSet.getString("constraint_schema"),
                                    resultSet.getString("constraint_table"),
                                    resultSet.getString("constraint_column"),
                                    resultSet.getString("referenced_schema"),
                                    resultSet.getString("referenced_table"),
                                    resultSet.getString("referenced_column")
                            );
                        }
                    }
                }
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public Table getTable(TableName tableName) {
        checkCatalog(tableName.getCatalog());

        try {
            List<Column> tableColumns = new ArrayList<>();
            List<ForeignKey> foreignKeys = new ArrayList<>();
            List<Index> indexes = new ArrayList<>();
            List<PrimaryKey> primaryKeys = new ArrayList<>();
            Connection connection = getConnection();
            String catalog = connection.getCatalog();
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT column_name,
                           is_identity,
                           c.data_type,
                           c.numeric_scale,
                           c.is_nullable
                    FROM information_schema.columns c
                    WHERE table_schema = ?
                    AND table_name = ?
                    ORDER BY ordinal_position""")) {
                preparedStatement.setString(1, tableName.getSchema());
                preparedStatement.setString(2, tableName.getTable());
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String columnName = resultSet.getString("column_name");
                        boolean isIdentity = "YES".equals(resultSet.getString("is_identity"));
                        String dataType = resultSet.getString("data_type");
                        int numericScale = resultSet.getInt("numeric_scale");
                        boolean nullable = "YES".equals(resultSet.getString("is_nullable"));
                        ColumnType columnType = getColumnType(dataType, numericScale);
                        tableColumns.add(
                                new Column(
                                        columnName,
                                        columnType,
                                        nullable,
                                        isIdentity
                                )
                        );
                    }
                }
            }

            // Collects the indexes
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT i.relname       AS index_name,
                           c.attname       AS column_name,
                           ix.indisunique  AS is_unique,
                           ix.indisprimary AS is_primary_key
                    FROM pg_catalog.pg_class t
                             JOIN pg_catalog.pg_namespace s ON s.oid = t.relnamespace
                             JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
                             JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
                             JOIN pg_catalog.pg_attribute c ON c.attrelid = t.oid
                    WHERE c.attnum = ANY (ix.indkey)
                      AND t.relkind = 'r'
                      AND s.nspname = ?
                      AND t.relname = ?
                    ORDER BY t.relname, i.relname, c.attnum""")) {
                preparedStatement.setString(1, tableName.getSchema());
                preparedStatement.setString(2, tableName.getTable());
                try (IndexCollector indexCollector = new IndexCollector(collector -> {
                    Index index = new Index(collector.getName(), tableName, collector.isUnique(), collector.isPrimaryKey(), collector.getColumns());
                    indexes.add(index);
                    if (collector.isPrimaryKey()) {
                        primaryKeys.add(new PrimaryKey(collector.getName(), collector.getColumns()));
                    }
                })) {
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            indexCollector.push(
                                    tableName.getSchema(),
                                    tableName.getTable(),
                                    resultSet.getString("index_name"),
                                    resultSet.getBoolean("is_unique"),
                                    resultSet.getBoolean("is_primary_key"),
                                    resultSet.getString("column_name")
                            );
                        }
                    }
                }
            }
            // Collects the foreign keys
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    WITH unnested_confkey AS (SELECT oid, UNNEST(confkey) AS confkey
                                              FROM pg_constraint),
                         unnested_conkey AS (SELECT oid, UNNEST(conkey) AS conkey
                                             FROM pg_constraint)
                    SELECT c.conname                      AS constraint_name,
                           col.attname                    AS constraint_column,
                           PG_GET_CONSTRAINTDEF(conf.oid) AS constraint_def,
                           rs.nspname                     AS referenced_schema,
                           rt.relname                     AS referenced_table,
                           rf.attname                     AS referenced_column
                    FROM pg_constraint c
                             JOIN unnested_conkey con ON c.oid = con.oid
                             JOIN pg_class t ON t.oid = c.conrelid
                             JOIN pg_catalog.pg_namespace s ON s.oid = t.relnamespace
                             JOIN pg_attribute col ON (col.attrelid = t.oid AND col.attnum = con.conkey)
                             JOIN pg_class rt ON c.confrelid = rt.oid
                             JOIN pg_catalog.pg_namespace rs ON rs.oid = rt.relnamespace
                             JOIN unnested_confkey conf ON c.oid = conf.oid
                             JOIN pg_attribute rf ON (rf.attrelid = c.confrelid AND rf.attnum = conf.confkey)
                    WHERE c.contype = 'f'
                      AND s.nspname = ?
                      AND t.relname = ?
                    ORDER BY c.conname, s.nspname, t.relname""")) {
                preparedStatement.setString(1, tableName.getSchema());
                preparedStatement.setString(2, tableName.getTable());
                try (ForeignKeyCollector foreignKeyCollector = new ForeignKeyCollector((constraint, constraintDef, fkSchema, fkTable, fkColumns, pkSchema, pkTable, pkColumns) -> {
                    TableName pkTableName = new TableName(catalog, pkSchema, pkTable);
                    ForeignKey foreignKey = new ForeignKey(
                            constraint,
                            constraintDef,
                            pkTableName,
                            pkColumns,
                            tableName,
                            fkColumns
                    );
                    foreignKeys.add(foreignKey);
                })) {
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            foreignKeyCollector.push(
                                    resultSet.getString("constraint_name"),
                                    resultSet.getString("constraint_def"),
                                    tableName.getSchema(),
                                    tableName.getTable(),
                                    resultSet.getString("constraint_column"),
                                    resultSet.getString("referenced_schema"),
                                    resultSet.getString("referenced_table"),
                                    resultSet.getString("referenced_column")
                            );
                        }
                    }
                }
            }

            return new Table(
                    tableName,
                    tableColumns,
                    indexes,
                    primaryKeys.isEmpty() ? null : primaryKeys.get(0),
                    foreignKeys);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ForeignKey> getRelatedForeignKeys(TableName tableName) {
        List<ForeignKey> foreignKeys = new ArrayList<>();
        checkCatalog(tableName.getCatalog());
        try (PreparedStatement preparedStatement = getConnection().prepareStatement("""
                WITH unnested_confkey AS (SELECT oid, UNNEST(confkey) AS confkey
                                          FROM pg_constraint),
                     unnested_conkey AS (SELECT oid, UNNEST(conkey) AS conkey
                                         FROM pg_constraint)
                SELECT c.conname                      AS constraint_name,
                       s.nspname                      AS constraint_schema,
                       t.relname                      AS constraint_table,
                       col.attname                    AS constraint_column,
                       PG_GET_CONSTRAINTDEF(conf.oid) AS constraint_def,
                       rs.nspname                     AS referenced_schema,
                       rt.relname                     AS referenced_table,
                       rf.attname                     AS referenced_column
                FROM pg_constraint c
                         JOIN unnested_conkey con ON c.oid = con.oid
                         JOIN pg_class t ON t.oid = c.conrelid
                         JOIN pg_catalog.pg_namespace s ON s.oid = t.relnamespace
                         JOIN pg_attribute col ON (col.attrelid = t.oid AND col.attnum = con.conkey)
                         JOIN pg_class rt ON c.confrelid = rt.oid
                         JOIN pg_catalog.pg_namespace rs ON rs.oid = rt.relnamespace
                         JOIN unnested_confkey conf ON c.oid = conf.oid
                         JOIN pg_attribute rf ON (rf.attrelid = c.confrelid AND rf.attnum = conf.confkey)
                WHERE c.contype = 'f'
                  AND rs.nspname = ?
                  AND rt.relname = ?
                ORDER BY c.conname, s.nspname, t.relname
                """)) {
            preparedStatement.setString(1, tableName.getSchema());
            preparedStatement.setString(2, tableName.getTable());
            try (ForeignKeyCollector foreignKeyCollector = new ForeignKeyCollector((constraint, constraintDef, fkSchema, fkTable, fkColumns, pkSchema, pkTable, pkColumns) -> {
                TableName pkTableName = new TableName(tableName.getCatalog(), pkSchema, pkTable);
                TableName fkTableName = new TableName(tableName.getCatalog(), fkSchema, fkTable);
                foreignKeys.add(new ForeignKey(
                        constraint,
                        constraintDef,
                        pkTableName,
                        pkColumns,
                        fkTableName,
                        fkColumns
                ));
            })) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        foreignKeyCollector.push(
                                resultSet.getString("constraint_name"),
                                resultSet.getString("constraint_def"),
                                resultSet.getString("constraint_schema"),
                                resultSet.getString("constraint_table"),
                                resultSet.getString("constraint_column"),
                                resultSet.getString("referenced_schema"),
                                resultSet.getString("referenced_table"),
                                resultSet.getString("referenced_column")
                        );
                    }
                }
            }
            return foreignKeys;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static ColumnType getColumnType(String dataType, int numericScale) {
        if (dataType.equals("character varying")) return ColumnType.VARCHAR;
        if (dataType.equals("character")) return ColumnType.VARCHAR;
        if (dataType.equals("text")) return ColumnType.VARCHAR;
        if (dataType.equals("timestamp without time zone"))
            return ColumnType.TIMESTAMP;
        if (dataType.equals("integer")) return ColumnType.INTEGER;
        if (dataType.equals("smallint")) return ColumnType.INTEGER;
        if (dataType.equals("bytea")) return ColumnType.INTEGER;
        if (dataType.equals("numeric"))
            if (numericScale == 0) return ColumnType.INTEGER;
            else return ColumnType.BIG_DECIMAL;

        throw new RuntimeException("Unexpected type: " + dataType);
    }

    @Override
    public DatabasePreparationFactory createDatabasePreparationFactory() {
        return DropForeignKeysPreparationStrategy::new;
    }

    @Override
    public RowCount getRowCount(TableName tableName) {
        return getRowCount("SELECT 1 FROM %s LIMIT (%d)".formatted(quote(tableName), ROW_COUNT_MAX + 1));
    }

    @Override
    public void enableForeignKey(ForeignKey foreignKey) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void disableForeignKey(ForeignKey foreignKey) {
        throw new RuntimeException("Not supported");
    }
}
