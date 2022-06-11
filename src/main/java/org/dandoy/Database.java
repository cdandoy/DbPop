package org.dandoy;

import org.apache.commons.csv.CSVRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class Database implements AutoCloseable {
    protected final Connection connection;
    private final Statement statement;
    private boolean verbose;

    protected Database(Connection connection) throws SQLException {
        this.connection = connection;
        statement = connection.createStatement();
    }

    public static Database createDatabase(Connection connection) throws SQLException {
        return new SqlServerDatabase(connection);
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    protected void executeSql(String sql, Object... args) {
        String s = String.format(sql, args);
        if (verbose) System.out.println("SQL: " + s);
        try {
            statement.execute(s);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract Collection<Table> getTables(Set<String> catalogs) throws SQLException;

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

    public DatabaseInserter createInserter(Table table, List<String> headerNames) throws SQLException {
        TableName tableName = table.getTableName();
        String sql = String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                quote(tableName),
                quote(headerNames),
                headerNames.stream().map(s -> "?").collect(Collectors.joining(", "))
        );
        return createInserter(table, sql);
    }

    protected String quote(Collection<String> strings) {
        return strings.stream()
                .map(this::quote)
                .collect(Collectors.joining(","));
    }

    protected String quote(TableName tableName) {
        return String.format(
                "%s.%s.%s",
                quote(tableName.getCatalog()),
                quote(tableName.getSchema()),
                quote(tableName.getTable())
        );
    }

    protected DatabaseInserter createInserter(Table table, String sql) throws SQLException {
        return new DatabaseInserter(sql);
    }

    protected abstract String quote(String s);

    public void deleteTable(Table table) {
        executeSql("DELETE FROM %s", quote(table.getTableName()));
    }

    public Database setVerbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public class DatabaseInserter implements AutoCloseable {
        private static final int BATCH_SIZE = 10000;
        protected final PreparedStatement preparedStatement;
        private int batched = 0;

        public DatabaseInserter(String sql) throws SQLException {
            preparedStatement = connection.prepareStatement(sql);
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
                preparedStatement.setString(i + 1, csvRecord.get(i));
            }
            preparedStatement.addBatch();
            if (batched++ > DatabaseInserter.BATCH_SIZE) {
                flush();
            }
        }
    }
}
