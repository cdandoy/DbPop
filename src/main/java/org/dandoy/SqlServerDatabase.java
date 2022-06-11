package org.dandoy;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class SqlServerDatabase extends Database {

    public SqlServerDatabase(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public Collection<Table> getTables(Set<String> catalogs) throws SQLException {
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
                            try (PreparedStatement tablesStatement = connection.prepareStatement("\n" +
                                    "SELECT s.name AS s, t.name AS t, c.name AS c, c.is_identity\n" +
                                    "FROM sys.schemas s\n" +
                                    "         JOIN sys.tables t ON t.schema_id = s.schema_id\n" +
                                    "         JOIN sys.columns c ON c.object_id = t.object_id\n" +
                                    "ORDER BY s.name, t.name, c.column_id")) {
                                try (TableCollector tableCollector = new TableCollector((schema, table, columns) -> {
                                    TableName tableName = new TableName(catalog, schema, table);
                                    tableColumns.put(tableName, columns);
                                })) {
                                    try (ResultSet tablesResultSet = tablesStatement.executeQuery()) {
                                        while (tablesResultSet.next()) {
                                            String schema = tablesResultSet.getString("s");
                                            String table = tablesResultSet.getString("t");
                                            String column = tablesResultSet.getString("c");
                                            boolean identity = tablesResultSet.getBoolean("is_identity");
                                            tableCollector.push(schema, table, new Column(column, identity));
                                        }
                                    }
                                }
                            }
                            try (PreparedStatement preparedStatement = connection.prepareStatement("\n" +
                                    "SELECT s.name AS s,\n" +
                                    "       t.name AS t,\n" +
                                    "       i.name AS i,\n" +
                                    "       i.is_unique,\n" +
                                    "       i.is_primary_key,\n" +
                                    "       i.has_filter,\n" +
                                    "       i.filter_definition,\n" +
                                    "       c.name AS c\n" +
                                    "FROM sys.schemas s\n" +
                                    "         JOIN sys.tables t ON t.schema_id = s.schema_id\n" +
                                    "         LEFT JOIN sys.indexes i ON i.object_id = t.object_id\n" +
                                    "         LEFT JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id\n" +
                                    "         LEFT JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id\n" +
                                    "WHERE i.type_desc IN ('NONCLUSTERED')\n" +
                                    "  AND i.is_disabled = 0\n" +
                                    "ORDER BY s.name, t.name, i.index_id, ic.key_ordinal")) {
                                try (IndexCollector indexCollector = new IndexCollector((schema, table, name, unique, primaryKey, filter, columns) -> {
                                    TableName tableName = new TableName(catalog, schema, table);
                                    Index index = new Index(name, tableName, unique, primaryKey, filter, columns);
                                    indexes.computeIfAbsent(tableName, it -> new ArrayList<>()).add(index);
                                })) {
                                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                                        while (resultSet.next()) {
                                            indexCollector.push(
                                                    resultSet.getString("s"),
                                                    resultSet.getString("t"),
                                                    resultSet.getString("i"),
                                                    resultSet.getBoolean("is_unique"),
                                                    resultSet.getBoolean("is_primary_key"),
                                                    resultSet.getBoolean("has_filter") ? resultSet.getString("filter_definition") : null,
                                                    resultSet.getString("c")
                                            );
                                        }
                                    }
                                }
                            }
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
                                try (ForeignKeyCollector foreignKeyCollector = new ForeignKeyCollector((constraint, fkSchema, fkTable, fkColumns, pkSchema, pkTable, pkColumns) -> {
                                    TableName pkTableName = new TableName(catalog, pkSchema, pkTable);
                                    TableName fkTableName = new TableName(catalog, fkSchema, fkTable);
                                    ForeignKey foreignKey = new ForeignKey(
                                            constraint,
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

    @Override
    public void identityInsert(TableName tableName, boolean enable) {
        executeSql(
                "SET IDENTITY_INSERT %s %s",
                quote(tableName),
                enable ? "ON" : "OFF"
        );
    }

    @Override
    protected SqlServerDatabaseInserter createInserter(Table table, String sql) throws SQLException {
        return new SqlServerDatabaseInserter(table, sql);
    }

    @Override
    protected String quote(String s) {
        return "[" + s + "]";
    }

    public void disableForeignKey(ForeignKey foreignKey) {
        executeSql(
                "ALTER TABLE %s NOCHECK CONSTRAINT %s",
                quote(foreignKey.getFkTableName()),
                foreignKey.getName()
        );
    }

    public void enableForeignKey(ForeignKey foreignKey) {
        executeSql(
                "ALTER TABLE %s WITH %s CHECK CONSTRAINT %s",
                quote(foreignKey.getFkTableName()),
                Settings.CHECK_CONTRAINTS ? "CHECK" : "NOCHECK",
                foreignKey.getName()
        );
    }

    static class TableCollector implements AutoCloseable {
        final TableConsumer tableConsumer;
        private String lastSchema;
        private String lastTable;
        private List<Column> columns;

        public TableCollector(TableConsumer tableConsumer) {
            this.tableConsumer = tableConsumer;
        }

        void push(String schema, String table, Column column) {
            if (!(table.equals(lastTable) && schema.equals(lastSchema))) {
                flush();
                lastSchema = schema;
                lastTable = table;
                columns = new ArrayList<>();
            }
            columns.add(column);
        }

        @Override
        public void close() {
            flush();
        }

        private void flush() {
            if (columns != null) {
                tableConsumer.consume(lastSchema, lastTable, columns);
            }
        }
    }

    interface TableConsumer {
        void consume(String schema, String table, List<Column> columns);
    }

    static class IndexCollector implements AutoCloseable {
        private final IndexConsumer indexConsumer;
        private String lastSchema;
        private String lastTable;
        private String lastIndex;
        private boolean lastUnique;
        private boolean lastPrimaryKey;
        private String lastFilter;
        private List<String> columns;

        IndexCollector(IndexConsumer indexConsumer) {
            this.indexConsumer = indexConsumer;
        }

        void push(String schema, String table, String index, boolean unique, boolean primaryKey, String filter, String column) {
            if (!(index.equals(lastIndex) && table.equals(lastTable) && schema.equals(lastSchema))) {
                flush();
                lastSchema = schema;
                lastTable = table;
                lastIndex = index;
                lastUnique = unique;
                lastPrimaryKey = primaryKey;
                lastFilter = filter;
                columns = new ArrayList<>();
            }
            columns.add(column);
        }

        @Override
        public void close() {
            flush();
        }

        private void flush() {
            if (columns != null) {
                indexConsumer.consume(lastSchema, lastTable, lastIndex, lastUnique, lastPrimaryKey, lastFilter, columns);
            }
        }
    }

    interface IndexConsumer {
        void consume(String schema, String table, String index, boolean unique, boolean primaryKey, String filter, List<String> columns);
    }

    static class ForeignKeyCollector implements AutoCloseable {
        private final ForeignKeyConsumer foreignKeyConsumer;
        private String lastConstraint;
        private String lastFkSchema;
        private String lastFkTable;
        private List<String> fkColumns;
        private String lastPkSchema;
        private String lastPkTable;
        private List<String> pkColumns;

        ForeignKeyCollector(ForeignKeyConsumer foreignKeyConsumer) {
            this.foreignKeyConsumer = foreignKeyConsumer;
        }

        @Override
        public void close() {
            flush();
        }

        void push(String constraint, String fkSchema, String fkTable, String fkColumn, String pkSchema, String pkTable, String pkColumn) {
            if (!(constraint.equals(lastConstraint) && fkSchema.equals(lastFkSchema) && fkTable.equals(lastFkTable))) {
                flush();
                lastConstraint = constraint;
                lastFkSchema = fkSchema;
                lastFkTable = fkTable;
                fkColumns = new ArrayList<>();
                lastPkSchema = pkSchema;
                lastPkTable = pkTable;
                pkColumns = new ArrayList<>();
            }
            fkColumns.add(fkColumn);
            pkColumns.add(pkColumn);
        }

        private void flush() {
            if (fkColumns != null) {
                foreignKeyConsumer.consume(lastConstraint, lastFkSchema, lastFkTable, fkColumns, lastPkSchema, lastPkTable, pkColumns);
            }
        }
    }

    interface ForeignKeyConsumer {
        void consume(String constraint, String fkSchema, String fkTable, List<String> fkColumns, String pkSchema, String pkTable, List<String> pkColumns);
    }

    class SqlServerDatabaseInserter extends DatabaseInserter {
        private final TableName tableName;
        private final boolean identity;

        public SqlServerDatabaseInserter(Table table, String sql) throws SQLException {
            super(sql);

            this.tableName = table.getTableName();
            identity = table.getColumns().stream().anyMatch(Column::isIdentity);
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
