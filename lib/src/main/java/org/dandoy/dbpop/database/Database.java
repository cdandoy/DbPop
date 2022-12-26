package org.dandoy.dbpop.database;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.database.mssql.SqlServerDatabase;
import org.dandoy.dbpop.database.pgsql.PostgresDatabase;
import org.dandoy.dbpop.upload.DataFileHeader;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpop.utils.StopWatch;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.dandoy.dbpop.database.ColumnType.INVALID;

@Slf4j
public abstract class Database implements AutoCloseable {
    private static final ExpressionParser EXPRESSION_PARSER = new ExpressionParser();
    private static final Base64.Decoder decoder = Base64.getDecoder();
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

    public abstract Table getTable(TableName tableName);

    public abstract List<ForeignKey> getRelatedForeignKeys(TableName tableName);

    public List<String> getSchemas(String catalog) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getSchemas(catalog, null)) {
                List<String> ret = new ArrayList<>();
                while (resultSet.next()) {
                    String name = resultSet.getString(1);
                    ret.add(name);
                }
                return ret;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

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
                        "ALTER TABLE %s ADD CONSTRAINT %s %s",
                        quote(foreignKey.getFkTableName()),
                        foreignKey.getName(),
                        constraintDef
                );
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to create the foreign key %s.%s", foreignKey.getFkTableName().toQualifiedName(), foreignKey.getName()), e);
        }
    }

    public DatabaseInserter createInserter(Table table, List<DataFileHeader> dataFileHeaders) throws SQLException {
        StringBuilder columnNames = new StringBuilder();
        StringBuilder bindVariables = new StringBuilder();

        for (DataFileHeader dataFileHeader : dataFileHeaders) {
            String columnName = dataFileHeader.getColumnName();
            Column column = table.getColumn(columnName);
            if (column != null) {
                ColumnType columnType = column.getColumnType();
                if (columnType != INVALID) {
                    if (columnNames.length() > 0) columnNames.append(",");
                    columnNames.append(quote(columnName));

                    if (bindVariables.length() > 0) bindVariables.append(",");
                    bindVariables.append("?");
                } else {
                    log.error("Cannot load the data type of {}.{}", table.tableName().toQualifiedName(), columnName);
                    dataFileHeader.setLoadable(false);
                }
            } else {
                log.error("Column not found: {}.{}", table.tableName().toQualifiedName(), columnName);
                dataFileHeader.setLoadable(false);
            }
        }

        String sql = String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                quote(table.tableName()),
                columnNames,
                bindVariables
        );
        return createInserter(table, dataFileHeaders, sql);
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

    public String quote(String s) {
        if (s.contains(identifierQuoteString)) {
            s = s.replace(identifierQuoteString, identifierQuoteString + identifierQuoteString);
        }
        return identifierQuoteString + s + identifierQuoteString;
    }

    public void deleteTable(Table table) {
        StopWatch.record("DELETE", () -> executeSql("DELETE FROM %s", quote(table.tableName())));
    }

    public abstract DatabasePreparationStrategy createDatabasePreparationStrategy(Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, List<String> datasets);

    public boolean isBinary(ResultSetMetaData metaData, int i) throws SQLException {
        int columnType = metaData.getColumnType(i + 1);
        return columnType == Types.BINARY ||
               columnType == Types.VARBINARY ||
               columnType == Types.LONGVARBINARY ||
               columnType == Types.BLOB;
    }

    /**
     * Searches for tables by partial name
     */
    public abstract Set<TableName> searchTable(String query);

    public class DatabaseInserter implements AutoCloseable {
        private static final int BATCH_SIZE = 10000;
        private final List<DataFileHeader> dataFileHeaders;
        private final PreparedStatement preparedStatement;
        private int batched = 0;
        private final List<ColumnInserter> columnInserters = new ArrayList<>();

        protected DatabaseInserter(Table table, List<DataFileHeader> dataFileHeaders, String sql) throws SQLException {
            this.dataFileHeaders = dataFileHeaders;
            preparedStatement = connection.prepareStatement(sql);

            int jdbcPos = 0;
            for (int i = 0; i < dataFileHeaders.size(); i++) {
                DataFileHeader dataFileHeader = dataFileHeaders.get(i);
                if (dataFileHeader.isLoadable()) {
                    jdbcPos++;
                    String columnName = dataFileHeader.getColumnName();
                    Column column = table.getColumn(columnName);
                    ColumnType columnType = column.getColumnType();
                    if (dataFileHeader.isBinary()) {
                        columnInserters.add(new BinaryColumnInserter(i, jdbcPos, columnType));
                    } else {
                        columnInserters.add(new RegularColumnInserter(i, jdbcPos, columnType));
                    }
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
            columnInserters.forEach(columnInserter -> columnInserter.consume(csvRecord));
            preparedStatement.addBatch();
            if (batched++ > DatabaseInserter.BATCH_SIZE) {
                flush();
            }
        }

        abstract class ColumnInserter {
            protected final int csvPos;
            protected final int jdbcPos;
            protected final ColumnType columnType;

            public ColumnInserter(int csvPos, int jdbcPos, ColumnType columnType) {
                this.csvPos = csvPos;
                this.jdbcPos = jdbcPos;
                this.columnType = columnType;
            }

            final void consume(CSVRecord csvRecord) {
                String s = csvRecord.get(csvPos);
                try {
                    consume(s);
                } catch (SQLException e) {
                    throw new RuntimeException(String.format(
                            "Failed to process column %s with value %s",
                            dataFileHeaders.get(csvPos).getColumnName(),
                            s
                    ), e);
                }
            }

            abstract void consume(String s) throws SQLException;
        }

        class RegularColumnInserter extends ColumnInserter {
            public RegularColumnInserter(int csvPos, int jdbcPos, ColumnType columnType) {
                super(csvPos, jdbcPos, columnType);
            }

            @Override
            void consume(String s) throws SQLException {
                if (s != null && s.startsWith("{{") && s.endsWith("}}")) {
                    Object value = EXPRESSION_PARSER.evaluate(s);
                    columnType.bind(preparedStatement, jdbcPos, value);
                } else {
                    columnType.bind(preparedStatement, jdbcPos, s);
                }
            }
        }

        class BinaryColumnInserter extends ColumnInserter {

            public BinaryColumnInserter(int csvPos, int jdbcPos, ColumnType columnType) {
                super(csvPos, jdbcPos, columnType);
            }

            @Override
            void consume(String s) throws SQLException {
                byte[] bytes;
                if (s != null) {
                    bytes = decoder.decode(s);
                } else {
                    bytes = null;
                }
                columnType.bind(preparedStatement, jdbcPos, bytes);
            }
        }
    }
}
