package org.dandoy.dbpop.database;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.upload.DataFileHeader;
import org.dandoy.dbpop.utils.StopWatch;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static org.dandoy.dbpop.database.ColumnType.INVALID;

@Slf4j
public abstract class DefaultDatabase extends Database {
    private static final ExpressionParser EXPRESSION_PARSER = new ExpressionParser();
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Base64.Decoder decoder = Base64.getDecoder();
    protected final SafeConnection safeConnection;
    private final String identifierQuoteString;

    protected DefaultDatabase(ConnectionBuilder connectionBuilder) {
        try {
            safeConnection = new SafeConnection(connectionBuilder, ConnectionVerifier.DEFAULT_CONNECTION_VERIFIER);
            DatabaseMetaData metaData = getConnection().getMetaData();
            identifierQuoteString = metaData.getIdentifierQuoteString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            safeConnection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void verifyConnection() {
        safeConnection.verifyConnection();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return safeConnection.getConnection();
    }

    protected void executeSql(String sql, Object... args) {
        String s = String.format(sql, args);
        try {
            log.debug("SQL: {}", s);
            Statement statement = safeConnection.getStatement();
            statement.execute(s);
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to execute \"%s\"", s), e);
        }
    }

    @Override
    public List<String> getSchemas(String catalog) {
        try {
            DatabaseMetaData metaData = getConnection().getMetaData();
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

    @Override
    public void dropForeignKey(ForeignKey foreignKey) {
        dropConstraint(foreignKey.getFkTableName(), foreignKey.getName());
    }

    protected void dropConstraint(TableName tableName, String constraint) {
        executeSql("ALTER TABLE %s DROP CONSTRAINT %s",
                quote(tableName),
                quote(constraint)
        );
    }

    @Override
    public void createForeignKey(ForeignKey foreignKey) {
        try {
            String constraintDef = foreignKey.getConstraintDef();
            if (constraintDef == null) {
                executeSql(
                        "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                        quote(foreignKey.getFkTableName()),
                        quote(foreignKey.getName()),
                        quote(",", foreignKey.getFkColumns()),
                        quote(foreignKey.getPkTableName()),
                        quote(",", foreignKey.getPkColumns())
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

    @Override
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
                    log.error("Cannot load the data type of {}.{}", table.getTableName().toQualifiedName(), columnName);
                    dataFileHeader.setLoadable(false);
                }
            } else {
                log.error("Column not found: {}.{}", table.getTableName().toQualifiedName(), columnName);
                dataFileHeader.setLoadable(false);
            }
        }

        String sql = String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                quote(table.getTableName()),
                columnNames,
                bindVariables
        );
        return createInserter(table, dataFileHeaders, sql);
    }

    @Override
    public String quote(String delimiter, String... strings) {
        return Arrays.stream(strings)
                .map(this::quote)
                .collect(Collectors.joining(delimiter));
    }

    @Override
    public String quote(String delimiter, Collection<String> strings) {
        return strings.stream()
                .map(this::quote)
                .collect(Collectors.joining(delimiter));
    }

    @Override
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

    @Override
    public String quote(String s) {
        if (s.contains(identifierQuoteString)) {
            s = s.replace(identifierQuoteString, identifierQuoteString + identifierQuoteString);
        }
        return identifierQuoteString + s + identifierQuoteString;
    }

    @Override
    public void deleteTable(Table table) {
        deleteTable(table.getTableName());
    }

    @Override
    public void deleteTable(TableName tableName) {
        StopWatch.record("DELETE", () -> executeSql("DELETE FROM %s", quote(tableName)));
    }

    @Override
    public boolean isBinary(ResultSetMetaData metaData, int i) throws SQLException {
        int columnType = metaData.getColumnType(i + 1);
        return columnType == Types.BINARY ||
               columnType == Types.VARBINARY ||
               columnType == Types.LONGVARBINARY ||
               columnType == Types.BLOB;
    }

    @NotNull
    protected RowCount getRowCount(String sql) {
        try (PreparedStatement preparedStatement = getConnection().prepareStatement(sql)) {
            int rows = 0;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                for (int i = 0; i < ROW_COUNT_MAX && resultSet.next(); i++) {
                    rows++;
                }
                boolean plus = resultSet.next();
                return new RowCount(rows, plus);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getTableDefinition(TableName tableName) {
        throw new RuntimeException("Not Implemented");
    }

    public class DatabaseInserter implements AutoCloseable {
        private static final int BATCH_SIZE = 10000;
        private final List<DataFileHeader> dataFileHeaders;
        private final PreparedStatement preparedStatement;
        private int batched = 0;
        private final List<ColumnInserter> columnInserters = new ArrayList<>();

        protected DatabaseInserter(Table table, List<DataFileHeader> dataFileHeaders, String sql) throws SQLException {
            this.dataFileHeaders = dataFileHeaders;
            preparedStatement = getConnection().prepareStatement(sql);

            addColumnInserters(table, dataFileHeaders);
        }

        private void addColumnInserters(Table table, List<DataFileHeader> dataFileHeaders) {
            int jdbcPos = 0;
            for (int csvPos = 0; csvPos < dataFileHeaders.size(); csvPos++) {
                DataFileHeader dataFileHeader = dataFileHeaders.get(csvPos);
                if (dataFileHeader.isLoadable()) {
                    jdbcPos++;
                    String columnName = dataFileHeader.getColumnName();
                    Column column = table.getColumn(columnName);
                    addColumnInserter(dataFileHeader, csvPos, jdbcPos, column);
                }
            }
        }

        protected void addColumnInserter(DataFileHeader dataFileHeader, int csvPos, int jdbcPos, Column column) {
            ColumnType columnType = column.getColumnType();
            ColumnInserter columnInserter = dataFileHeader.isBinary() ?
                    new BinaryColumnInserter(csvPos, jdbcPos, columnType) :
                    new RegularColumnInserter(csvPos, jdbcPos, columnType);
            addColumnInserter(columnInserter);
        }

        protected void addColumnInserter(ColumnInserter columnInserter) {
            columnInserters.add(columnInserter);
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

        public abstract class ColumnInserter {
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

            protected abstract void consume(String s) throws SQLException;
        }

        class RegularColumnInserter extends ColumnInserter {
            public RegularColumnInserter(int csvPos, int jdbcPos, ColumnType columnType) {
                super(csvPos, jdbcPos, columnType);
            }

            @Override
            protected void consume(String s) throws SQLException {
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
            protected void consume(String s) throws SQLException {
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

    protected interface ConnectionVerifier {
        ConnectionVerifier DEFAULT_CONNECTION_VERIFIER = connection -> {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1")) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        return true;
                    }
                }
            }
            return false;
        };

        boolean verifyConnection(Connection connection) throws Exception;
    }

    protected static class SafeConnection implements AutoCloseable {
        protected final ConnectionBuilder connectionBuilder;
        private final ConnectionVerifier connectionVerifier;
        private Connection connection;
        private Statement statement;

        protected SafeConnection(ConnectionBuilder connectionBuilder, ConnectionVerifier connectionVerifier) {
            this.connectionBuilder = connectionBuilder;
            this.connectionVerifier = connectionVerifier;
        }

        @Override
        public void close() throws SQLException {
            safeClose(connection);
            safeClose(statement);
        }

        private static void safeClose(AutoCloseable autoCloseable) {
            if (autoCloseable != null) {
                try {
                    autoCloseable.close();
                } catch (Exception ignored) {
                }
            }
        }

        public synchronized void verifyConnection() {
            try {
                if (connection != null && connectionVerifier.verifyConnection(connection))
                    return;
            } catch (Exception e) {
                log.error("verifyConnection failed", e);
            }
            resetConnection();
        }

        private void resetConnection() {
            log.info("Resetting the connection");
            safeClose(connection);
            connection = null;
            safeClose(statement);
            statement = null;

            try {
                getConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public synchronized Connection getConnection() throws SQLException {
            if (connection == null) {
                connection = connectionBuilder.createConnection();
                connection.setAutoCommit(true);
            }
            return connection;
        }

        public synchronized Statement getStatement() throws SQLException {
            if (statement == null) {
                statement = getConnection().createStatement();
            }
            return statement;
        }
    }
}
