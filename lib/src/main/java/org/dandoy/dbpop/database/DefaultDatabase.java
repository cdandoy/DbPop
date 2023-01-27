package org.dandoy.dbpop.database;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.upload.DataFileHeader;
import org.dandoy.dbpop.utils.StopWatch;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.dandoy.dbpop.database.ColumnType.INVALID;

@Slf4j
public abstract class DefaultDatabase extends Database {
    private static final ExpressionParser EXPRESSION_PARSER = new ExpressionParser();
    private static final Base64.Decoder decoder = Base64.getDecoder();
    protected final Connection connection;
    protected final Statement statement;
    private final String identifierQuoteString;

    protected DefaultDatabase(Connection connection) {
        try {
            this.connection = connection;
            statement = connection.createStatement();
            DatabaseMetaData metaData = connection.getMetaData();
            identifierQuoteString = metaData.getIdentifierQuoteString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
            statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
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

    @Override
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
    public String quote(Collection<String> strings) {
        return strings.stream()
                .map(this::quote)
                .collect(Collectors.joining(","));
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
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
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
