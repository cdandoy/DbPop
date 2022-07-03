package org.dandoy.dbpop.database;

import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.upload.DataFileHeader;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public abstract class Database implements AutoCloseable {
    protected final Connection connection;
    private final Statement statement;

    protected Database(Connection connection) {
        try {
            this.connection = connection;
            statement = connection.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Database createDatabase(Connection connection) {
        return new SqlServerDatabase(connection);
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
            statement.execute(s);
        } catch (SQLException e) {
            throw new RuntimeException(e);
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

    public void dropIndex(Index index) {
        TableName tableName = index.getTableName();
        try {
            if (index.isPrimaryKey()) {
                dropConstraint(tableName, index.getName());
            } else {
                executeSql(
                        "DROP INDEX %s ON %s",
                        quote(index.getName()),
                        quote(tableName)
                );
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to drop the index %s.%s", tableName.toQualifiedName(), index.getName()), e);
        }
    }

    public void createIndex(Index index) {
        TableName tableName = index.getTableName();
        if (index.isPrimaryKey()) {
            executeSql(
                    "ALTER TABLE %s ADD CONSTRAINT %s %s (%s)",
                    quote(tableName),
                    quote(index.getName()),
                    index.isPrimaryKey() ? "PRIMARY KEY" : "UNIQUE",
                    quote(index.getColumns())
            );
        } else {
            executeSql(
                    "CREATE %sINDEX %s ON %s (%s)",
                    index.isUnique() ? "UNIQUE " : "",
                    quote(index.getName()),
                    quote(tableName),
                    quote(index.getColumns())
            );
        }
    }

    public void createForeignKey(ForeignKey foreignKey) {
        try {
            executeSql(
                    "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                    quote(foreignKey.getFkTableName()),
                    quote(foreignKey.getName()),
                    quote(foreignKey.getFkColumns()),
                    quote(foreignKey.getPkTableName()),
                    quote(foreignKey.getPkColumns())
            );
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to create the foreign key %s.%s", foreignKey.getFkTableName().toQualifiedName(), foreignKey.getName()), e);
        }
    }

    public void truncateTable(Table table) {
        executeSql("TRUNCATE TABLE %s", quote(table.getTableName()));
    }

    public abstract void identityInsert(TableName tableName, boolean enable);

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
        return new DatabaseInserter(dataFileHeaders, sql);
    }

    protected abstract String quote(String s);

    void deleteTable(Table table) {
        executeSql("DELETE FROM %s", quote(table.getTableName()));
    }

    public abstract DatabasePreparationStrategy createDatabasePreparationStrategy(Map<TableName, Table> tablesByName, Set<Table> loadedTables);

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
        private int batched = 0;

        DatabaseInserter(List<DataFileHeader> dataFileHeaders, String sql) throws SQLException {
            preparedStatement = connection.prepareStatement(sql);

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
            connection.commit();
            batched = 0;
        }

        public void insert(CSVRecord csvRecord) throws SQLException {
            for (int i = 0; i < csvRecord.size(); i++) {
                String s = csvRecord.get(i);
                if (binaryColumns.contains(i)) {
                    byte[] bytes;
                    if (s != null) {
                        Base64.Decoder decoder = Base64.getDecoder();
                        bytes = decoder.decode(s);
                    } else {
                        bytes = null;
                    }
                    preparedStatement.setBytes(i + 1, bytes);
                } else {
                    preparedStatement.setString(i + 1, s);
                }
            }
            preparedStatement.addBatch();
            if (batched++ > DatabaseInserter.BATCH_SIZE) {
                flush();
            }
        }
    }
}
