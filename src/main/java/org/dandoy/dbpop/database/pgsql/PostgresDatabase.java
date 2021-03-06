package org.dandoy.dbpop.database.pgsql;

import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.database.utils.ForeignKeyCollector;
import org.dandoy.dbpop.database.utils.IndexCollector;
import org.dandoy.dbpop.database.utils.TableCollector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class PostgresDatabase extends Database {

    public PostgresDatabase(Connection connection) {
        super(connection);
    }

    private void checkCatalog(String catalog) {
        try {
            if (catalog != null && !catalog.equals(connection.getCatalog())) {
                throw new RuntimeException(String.format(
                        "Cannot reach database %s when connected to database %s",
                        catalog, connection.getCatalog()
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

            try (PreparedStatement preparedStatement = connection.prepareStatement("\n" +
                    "SELECT table_catalog,\n" +
                    "       table_schema,\n" +
                    "       table_name\n" +
                    "FROM information_schema.tables t\n" +
                    "WHERE table_type = 'BASE TABLE'\n" +
                    "  AND table_schema = ?")) {
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
    public Collection<Table> getTables(Set<TableName> datasetTableNames) {
        datasetTableNames.stream().map(TableName::getCatalog).forEach(this::checkCatalog);

        try {
            Map<TableName, List<Column>> tableColumns = new HashMap<>();
            Map<TableName, List<ForeignKey>> foreignKeys = new HashMap<>();
            Map<TableName, List<Index>> indexes = new HashMap<>();
            String catalog = connection.getCatalog();
            try (PreparedStatement preparedStatement = connection.prepareStatement("" +
                    "SELECT table_schema,\n" +
                    "       table_name,\n" +
                    "       column_name,\n" +
                    "       is_identity,\n" +
                    "       c.data_type,\n" +
                    "       c.numeric_scale,\n" +
                    "       c.is_nullable\n" +
                    "FROM information_schema.columns c\n" +
                    "WHERE table_schema NOT IN ('information_schema', 'pg_catalog')\n" +
                    "ORDER BY table_catalog, table_schema, table_name, ordinal_position")) {
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
            try (PreparedStatement preparedStatement = connection.prepareStatement("" +
                    "SELECT s.nspname       AS schema_name,\n" +
                    "       t.relname       AS table_name,\n" +
                    "       i.relname       AS index_name,\n" +
                    "       c.attname       AS column_name,\n" +
                    "       ix.indisunique  AS is_unique,\n" +
                    "       ix.indisprimary AS is_primary_key\n" +
                    "FROM pg_catalog.pg_class t\n" +
                    "         JOIN pg_catalog.pg_namespace s ON s.oid = t.relnamespace\n" +
                    "         JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid\n" +
                    "         JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid\n" +
                    "         JOIN pg_catalog.pg_attribute c ON c.attrelid = t.oid\n" +
                    "WHERE c.attnum = ANY (ix.indkey)\n" +
                    "  AND t.relkind = 'r'\n" +
                    "ORDER BY t.relname, i.relname, c.attnum")) {
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
            try (PreparedStatement preparedStatement = connection.prepareStatement("" +
                    "WITH unnested_confkey AS (SELECT oid, UNNEST(confkey) AS confkey\n" +
                    "                          FROM pg_constraint),\n" +
                    "     unnested_conkey AS (SELECT oid, UNNEST(conkey) AS conkey\n" +
                    "                         FROM pg_constraint)\n" +
                    "SELECT c.conname                      AS constraint_name,\n" +
                    "       s.nspname                      AS constraint_schema,\n" +
                    "       t.relname                      AS constraint_table,\n" +
                    "       col.attname                    AS constraint_column,\n" +
                    "       PG_GET_CONSTRAINTDEF(conf.oid) AS constraint_def,\n" +
                    "       rs.nspname                     AS referenced_schema,\n" +
                    "       rt.relname                     AS referenced_table,\n" +
                    "       rf.attname                   " +
                    "  AS referenced_column\n" +
                    "FROM pg_constraint c\n" +
                    "         JOIN unnested_conkey con ON c.oid = con.oid\n" +
                    "         JOIN pg_class t ON t.oid = c.conrelid\n" +
                    "         JOIN pg_catalog.pg_namespace s ON s.oid = t.relnamespace\n" +
                    "         JOIN pg_attribute col ON (col.attrelid = t.oid AND col.attnum = con.conkey)\n" +
                    "         JOIN pg_class rt ON c.confrelid = rt.oid\n" +
                    "         JOIN pg_catalog.pg_namespace rs ON rs.oid = rt.relnamespace\n" +
                    "         JOIN unnested_confkey conf ON c.oid = conf.oid\n" +
                    "         JOIN pg_attribute rf ON (rf.attrelid = c.confrelid AND rf.attnum = conf.confkey)\n" +
                    "WHERE c.contype = 'f'\n" +
                    "ORDER BY c.conname, s.nspname, t.relname")) {
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
                    .map(entry -> new Table(
                            entry.getKey(),
                            entry.getValue(),
                            indexes.computeIfAbsent(entry.getKey(), tableName -> Collections.emptyList()),
                            foreignKeys.computeIfAbsent(entry.getKey(), tableName -> Collections.emptyList()))
                    )
                    .collect(Collectors.toList());
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
    public DatabasePreparationStrategy<PostgresDatabase> createDatabasePreparationStrategy(Map<TableName, Table> tablesByName, Set<Table> loadedTables) {
        return new PostgresDatabasePreparationStrategy(this, tablesByName);
    }
}
