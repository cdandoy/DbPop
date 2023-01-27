package org.dandoy.dbpop.download;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ColumnType;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.PrimaryKey;
import org.dandoy.dbpop.database.Table;

import java.sql.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class TableFetcher implements AutoCloseable {
    public static final int BATCH_SIZE = 100;
    private final Database database;
    private final String sql;
    private final PreparedStatement preparedStatement;
    private final int skipBind;
    private final int batchSize;
    private final List<ColumnType> pkColumnTypes;
    private final ExecutionContext executionContext;

    private TableFetcher(Database database, String sql, PreparedStatement preparedStatement, int skipBind, int batchSize, List<ColumnType> pkColumnTypes, ExecutionContext executionContext) {
        this.database = database;
        this.sql = sql;
        this.preparedStatement = preparedStatement;
        this.skipBind = skipBind;
        this.batchSize = batchSize;
        this.pkColumnTypes = pkColumnTypes;
        this.executionContext = executionContext;
    }

    public static TableFetcher createTableFetcher(Database database, Table table, List<TableJoin> tableJoins, List<TableQuery> where, List<String> filteredColumns, ExecutionContext executionContext) {
        String sql = ("SELECT %s.*\nFROM %s").formatted(
                database.quote(table.getTableName()),
                database.quote(table.getTableName())
        );

        String joins = tableJoins.stream()
                .map(tableJoin -> {
                    String join = tableJoin.tableConditions().stream()
                            .map(tableCondition -> "(%s.%s = %s.%s)".formatted(
                                    database.quote(tableJoin.leftTable()),
                                    database.quote(tableCondition.leftColumn()),
                                    database.quote(tableJoin.rightTable()),
                                    database.quote(tableCondition.rightColumn())
                            ))
                            .collect(Collectors.joining(" AND "));
                    return "\nJOIN %s ON %s".formatted(
                            database.quote(tableJoin.leftTable()),
                            join
                    );
                })
                .collect(Collectors.joining("\n"));
        sql += joins;

        String whereClause = where.stream()
                .map(tableQuery -> "(%s.%s = ?)".formatted(
                        database.quote(tableQuery.tableName()),
                        database.quote(tableQuery.column())
                ))
                .collect(Collectors.joining(" AND "));

        String childFilterClause = "";
        int batchSize = Integer.MAX_VALUE;
        List<ColumnType> pkColumnTypes = null;
        if (!filteredColumns.isEmpty()) {
            String pkWhereClause = filteredColumns.stream()
                    .map(filteredColumn -> "(%s.%s = ?)".formatted(
                            database.quote(table.getTableName()),
                            filteredColumn
                    ))
                    .collect(Collectors.joining(" AND "));
            batchSize = BATCH_SIZE / filteredColumns.size();
            childFilterClause = Stream.generate(() -> pkWhereClause)
                    .limit(batchSize)
                    .collect(Collectors.joining("\nOR "));
            pkColumnTypes = filteredColumns.stream().map(it -> table.getColumn(it).getColumnType()).toList();
        }
        if (!whereClause.isEmpty() || !childFilterClause.isEmpty()) {
            sql = sql + "\nWHERE " + Stream.of(whereClause, childFilterClause)
                    .filter(it -> !it.isEmpty())
                    .map(it -> "(" + it + ")")
                    .collect(Collectors.joining(" AND "));
        }

        // We want to sort our CSV files as much as possible to make it easier to see the changes
        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey != null) {
            sql = "%s\nORDER BY %s".formatted(
                    sql,
                    String.join(",", primaryKey.columns())
            );
        }

        log.debug(sql);

        Connection connection = database.getConnection();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < where.size(); i++) {
                preparedStatement.setString(i + 1, where.get(i).value());
            }
            return new TableFetcher(database, sql, preparedStatement, where.size(), batchSize, pkColumnTypes, executionContext);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute " + sql, e);
        }
    }

    @Override
    public void close() {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void execute(Set<List<Object>> pks, Predicate<ResultSet> predicate) {
        try {
            if (!pks.isEmpty()) {
                List<List<Object>> pks2 = new LinkedList<>(pks);
                while (!pks2.isEmpty()) {
                    int split = Math.min(batchSize, pks2.size());
                    List<List<Object>> todo = pks2.subList(0, split);
                    pks2 = pks2.subList(split, pks2.size());
                    executeSublist(todo, predicate);
                }
            } else {
                executeSublist(Collections.emptyList(), predicate);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void bind(List<List<Object>> pks) throws SQLException {
        int jdbcPos = 1;
        for (List<Object> pk : pks) {
            for (Object o : pk) {
                preparedStatement.setObject(skipBind + jdbcPos++, o);
            }
        }
        while (jdbcPos <= batchSize) {
            for (ColumnType columnType : pkColumnTypes) {
                preparedStatement.setNull(skipBind + jdbcPos++, columnType.toSqlType());
            }
        }
    }

    private void executeSublist(List<List<Object>> pks, Predicate<ResultSet> consumer) throws SQLException {
        if (!pks.isEmpty()) {
            bind(pks);
        }
        int rowsLeft = executionContext.getTotalRowCountLimit() - executionContext.getTotalRowCount();
        if (rowsLeft <= 0) return;
        preparedStatement.setMaxRows(rowsLeft);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                if (!consumer.test(resultSet)) {
                    break;
                }
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
}
