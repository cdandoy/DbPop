package org.dandoy.dbpop.database;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.database.mssql.SqlServerDatabase;
import org.dandoy.dbpop.database.pgsql.PostgresDatabase;
import org.dandoy.dbpop.upload.DataFileHeader;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public abstract class Database implements AutoCloseable {
    private static final ExpressionParser EXPRESSION_PARSER = new ExpressionParser();
    protected final Connection connection;
    protected final Statement statement;
    private final String identifierQuoteString;

    protected Database(Connection connection) {
        try {
            this.connection = connection;
            statement = connection.createStatement();
            DatabaseMetaData metaData = connection.getMetaData();
            identifierQuoteString = metaData.getIdentifierQuoteString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Database createDatabase(Connection connection) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            String databaseProductName = metaData.getDatabaseProductName();
            if ("Microsoft SQL Server".equals(databaseProductName)) {
                return new SqlServerDatabase(connection);
            } else if ("PostgreSQL".equals(databaseProductName)) {
                return new PostgresDatabase(connection);
            } else {
                throw new RuntimeException("Unsupported database " + databaseProductName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    protected void executeSql(String sql, Object... args) {
        String s = String.format(sql, args);
        try {
            log.debug("SQL: {}", s);
            statement.execute(s);
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to execute \"%s\"", s), e);
        }
    }

    public abstract Collection<TableName> getTableNames(String catalog, String schema);

    public abstract Collection<Table> getTables(Set<TableName> datasetTableNames);

    public void dropForeignKey(ForeignKey foreignKey) {
        dropConstraint(foreignKey.getFkTableName(), foreignKey.getName());
    }

    private void dropConstraint(TableName tableName, String constraint) {
        executeSql("ALTER TABLE %s DROP CONSTRAINT %s",
                quote(tableName),
                quote(constraint)
        );
    }

    public void createForeignKey(ForeignKey foreignKey) {
        try {
            String constraintDef = foreignKey.getConstraintDef();
            if (constraintDef == null) {
                executeSql(
                        "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                        quote(foreignKey.getFkTableName()),
                        quote(foreignKey.getName()),
                        quote(foreignKey.getFkColumns()),
                        quote(foreignKey.getPkTableName()),
                        quote(foreignKey.getPkColumns())
                );
            } else {
                executeSql(
                        "ALTER TABLE %s ADD %s",
                        quote(foreignKey.getFkTableName()),
                        constraintDef
                );
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to create the foreign key %s.%s", foreignKey.getFkTableName().toQualifiedName(), foreignKey.getName()), e);
        }
    }

    public DatabaseInserter createInserter(Table table, List<DataFileHeader> dataFileHeaders) throws SQLException {
        TableName tableName = table.getTableName();
        String sql = String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                quote(tableName),
                quoteDataFileHeader(dataFileHeaders),
                dataFileHeaders.stream().map(s -> "?").collect(Collectors.joining(", "))
        );
        return createInserter(table, dataFileHeaders, sql);
    }

    public String quoteDataFileHeader(Collection<DataFileHeader> strings) {
        return strings.stream()
                .map(dataFileHeader -> quote(dataFileHeader.getColumnName()))
                .collect(Collectors.joining(","));
    }

    public String quote(Collection<String> strings) {
        return strings.stream()
                .map(this::quote)
                .collect(Collectors.joining(","));
    }

    public String quote(TableName tableName) {
        return String.format(
                "%s.%s.%s",
                quote(tableName.getCatalog()),
                quote(tableName.getSchema()),
                quote(tableName.getTable())
        );
    }

    protected DatabaseInserter createInserter(Table table, List<DataFileHeader> dataFileHeaders, String sql) throws SQLException {
        return new DatabaseInserter(table, dataFileHeaders, sql);
    }

    protected String quote(String s) {
        if (s.contains(identifierQuoteString)) {
            s = s.replace(identifierQuoteString, identifierQuoteString + identifierQuoteString);
        }
        return identifierQuoteString + s + identifierQuoteString;
    }

    public void deleteTable(Table table) {
        executeSql("DELETE FROM %s", quote(table.getTableName()));
    }

    public abstract DatabasePreparationStrategy<? extends Database> createDatabasePreparationStrategy(Map<TableName, Table> tablesByName, Set<Table> loadedTables);

    public boolean isBinary(ResultSetMetaData metaData, int i) throws SQLException {
        int columnType = metaData.getColumnType(i + 1);
        return columnType == Types.BINARY ||
                columnType == Types.VARBINARY ||
                columnType == Types.LONGVARBINARY ||
                columnType == Types.BLOB;
    }

    public class DatabaseInserter implements AutoCloseable {
        private static final int BATCH_SIZE = 10000;
        private final PreparedStatement preparedStatement;
        private final List<Integer> binaryColumns = new ArrayList<>();
        private final List<ColumnType> columnTypes;
        private int batched = 0;

        protected DatabaseInserter(Table table, List<DataFileHeader> dataFileHeaders, String sql) throws SQLException {
            preparedStatement = connection.prepareStatement(sql);

            List<Column> columns = table.getColumns();
            columnTypes = dataFileHeaders.stream()
                    .map(dataFileHeader -> {
                        String columnName = dataFileHeader.getColumnName();
                        for (Column column : columns) {
                            if (column.getName().equals(columnName)) {
                                return column.getColumnType();
                            }
                        }
                        throw new RuntimeException("Column doesn't exist: " + columnName);
                    })
                    .collect(Collectors.toList());
            for (int i = 0; i < dataFileHeaders.size(); i++) {
                if (dataFileHeaders.get(i).isBinary()) {
                    binaryColumns.add(i);
                }
            }
        }

        @Override
        public void close() throws SQLException {
            flush();
            preparedStatement.close();
        }

        private void flush() throws SQLException {
            preparedStatement.executeBatch();
            batched = 0;
        }

        public void insert(CSVRecord csvRecord) throws SQLException {
            for (int i = 0; i < csvRecord.size(); i++) {
                int jdbcPos = i + 1;
                String s = csvRecord.get(i);
                ColumnType columnType = columnTypes.get(i);
                if (binaryColumns.contains(i)) {
                    byte[] bytes;
                    if (s != null) {
                        Base64.Decoder decoder = Base64.getDecoder();
                        bytes = decoder.decode(s);
                    } else {
                        bytes = null;
                    }
                    columnType.bind(preparedStatement, jdbcPos, bytes);
                } else {
                    if (s != null && s.startsWith("{{") && s.endsWith("}}")) {
                        Object value = EXPRESSION_PARSER.evaluate(s);
                        columnType.bind(preparedStatement, jdbcPos, value);
                    } else {
                        columnType.bind(preparedStatement, jdbcPos, s);
                    }
                }
            }
            preparedStatement.addBatch();
            if (batched++ > DatabaseInserter.BATCH_SIZE) {
                flush();
            }
        }
    }
}
