package org.dandoy.dbpop.download2;

import org.dandoy.dbpop.database.ColumnType;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Table;

import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableExecutor implements AutoCloseable{
    private final Database database;
    private final String sql;
    private final PreparedStatement preparedStatement;
    private final int batchSize;
    private final List<ColumnType> pkColumnTypes;
    private final boolean byPrimaryKey;

    private TableExecutor(Database database, String sql, PreparedStatement preparedStatement, int batchSize, List<ColumnType> pkColumnTypes, boolean byPrimaryKey) {
        this.database = database;
        this.sql = sql;
        this.preparedStatement = preparedStatement;
        this.batchSize = batchSize;
        this.pkColumnTypes = pkColumnTypes;
        this.byPrimaryKey = byPrimaryKey;
    }

    public static TableExecutor createTableExecutor(Database database, Table table, boolean byPrimaryKey) {
        String sql = ("SELECT *\nFROM %s").formatted(database.quote(table.tableName()));
        int batchSize = Integer.MAX_VALUE;
        List<ColumnType> pkColumnTypes = null;
        if (byPrimaryKey) {
            List<String> pkColumnNames = table.primaryKey().columns();
            String pkWhereClause = pkColumnNames.stream()
                    .map("(%s = ?)"::formatted)
                    .collect(Collectors.joining(" AND "));
            batchSize = 2000 / pkColumnNames.size();
            String whereClause = Stream.generate(() -> pkWhereClause)
                    .limit(batchSize)
                    .collect(Collectors.joining("\nOR "));
            sql = sql + "\nWHERE " + whereClause;
            pkColumnTypes = pkColumnNames.stream().map(it -> table.getColumn(it).getColumnType()).toList();
        }
        Connection connection = database.getConnection();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            return new TableExecutor(database, sql, preparedStatement, batchSize, pkColumnTypes, byPrimaryKey);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute " + sql, e);
        }
    }

    @Override
    public void close()  {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void execute(Set<List<Object>> pks, Consumer<ResultSet> consumer) {
        try {
            if (byPrimaryKey) {
                List<List<Object>> pks2 = new LinkedList<>(pks);
                while (!pks2.isEmpty()) {
                    int split = Math.min(batchSize, pks2.size());
                    List<List<Object>> todo = pks2.subList(0, split);
                    pks2 = pks2.subList(split, pks2.size());
                    executeSublist(todo, consumer);
                }
            } else {
                execute(consumer);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void execute(Consumer<ResultSet> consumer) {
        if (byPrimaryKey) throw new RuntimeException("execute(Consumer) only handles full downloads");

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                consumer.accept(resultSet);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void bind(List<List<Object>> pks) throws SQLException {
        int jdbcPos = 1;
        for (List<Object> pk : pks) {
            for (Object o : pk) {
                preparedStatement.setObject(jdbcPos++, o);
            }
        }
        while (jdbcPos <= batchSize) {
            for (ColumnType columnType : pkColumnTypes) {
                preparedStatement.setObject(jdbcPos++, columnType.toSqlType());
            }
        }
    }

    private void executeSublist(List<List<Object>> pks, Consumer<ResultSet> consumer) throws SQLException {
        bind(pks);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                consumer.accept(resultSet);
            }
        }
    }

    public List<SelectedColumn> getSelectedColumns() {
        List<SelectedColumn> ret = new ArrayList<>();
        try {
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                String columnTypeName = metaData.getColumnTypeName(i + 1);
                int precision = metaData.getPrecision(i + 1);
                ColumnType columnType = ColumnType.getColumnType(columnTypeName, precision);
                boolean binary = database.isBinary(metaData, i);
                ret.add(new SelectedColumn(i + 1, columnName, columnType, binary));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get the metadata of " + sql, e);
        }
        return ret;
    }

    public record SelectedColumn(int jdbcPos, String name, ColumnType columnType, boolean binary) {
        public String asHeaderName() {
            if (binary) return name + "*b64";
            return name;
        }

        public static SelectedColumn findByName(Collection<SelectedColumn> selectedColumns, String columnName) {
            for (SelectedColumn selectedColumn : selectedColumns) {
                if (columnName.equals(selectedColumn.name)) {
                    return selectedColumn;
                }
            }
            return null;
        }
    }
}
