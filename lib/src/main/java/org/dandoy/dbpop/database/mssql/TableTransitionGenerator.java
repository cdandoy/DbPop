package org.dandoy.dbpop.database.mssql;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.Transition;
import org.dandoy.dbpop.database.TransitionGenerator;
import org.dandoy.dbpop.utils.CollectionComparator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableTransitionGenerator extends TransitionGenerator {
    public TableTransitionGenerator(SqlServerDatabase database) {
        super(database);
    }

    @Override
    public String drop(ObjectIdentifier objectIdentifier) {
        return "DROP TABLE %s".formatted(database.quote(objectIdentifier));
    }

    @Override
    protected void update(ObjectIdentifier objectIdentifier, String fromSql, String toSql, Transition transition) {
        CreateTable fromStatement = parseCreateTable(fromSql);
        CreateTable toStatement = parseCreateTable(toSql);
        Map<String, ColumnDefinition> fromColumns = toColumnDefinitionMap(fromStatement);
        Map<String, ColumnDefinition> toColumns = toColumnDefinitionMap(toStatement);
        updateColumns(objectIdentifier, fromColumns, toColumns, transition);
    }

    private static Map<String, ColumnDefinition> toColumnDefinitionMap(CreateTable createTable) {
        Map<String, ColumnDefinition> ret = new LinkedHashMap<>();
        for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
            ret.put(columnDefinition.getColumnName(), columnDefinition);
        }
        return ret;
    }

    private void updateColumns(ObjectIdentifier objectIdentifier, Map<String, ColumnDefinition> fromColumns, Map<String, ColumnDefinition> toColumns, Transition transition) {
        CollectionComparator<String> comparator = CollectionComparator.build(fromColumns.keySet(), toColumns.keySet());

        // DROP
        if (!comparator.leftOnly.isEmpty()) {
            String sql = "ALTER TABLE %s DROP COLUMN %s".formatted(
                    database.quote(objectIdentifier),
                    String.join(", ", comparator.leftOnly)
            );
            transition.addSql(sql);
        }

        // UPDATE
        comparator.common.stream()
                .filter(columnName -> {
                    ColumnDefinition fromColumnDefinition = fromColumns.get(columnName);
                    ColumnDefinition toColumnDefinition = toColumns.get(columnName);
                    String fromSql = fromColumnDefinition.toStringDataTypeAndSpec();
                    String toSql = toColumnDefinition.toStringDataTypeAndSpec();
                    return !fromSql.equals(toSql);
                })
                .map(columnName -> "ALTER TABLE %s ALTER COLUMN %s %s".formatted(
                        database.quote(objectIdentifier),
                        columnName,
                        toColumns.get(columnName).toStringDataTypeAndSpec()
                ))
                .forEach(transition::addSql);

        // ADD
        if (!comparator.rightOnly.isEmpty()) {
            String sql = "ALTER TABLE %s ADD %s".formatted(
                    database.quote(objectIdentifier),
                    comparator.rightOnly.stream()
                            .map(toColumns::get)
                            .map(columnDefinition ->
                                    "%s %s".formatted(
                                            columnDefinition.getColumnName(),
                                            columnDefinition.toStringDataTypeAndSpec()
                                    )
                            )
                            .collect(Collectors.joining(", "))
            );
            transition.addSql(sql);
        }
    }

    private CreateTable parseCreateTable(String sql) {
        try {
            try {
                Statement statement = CCJSqlParserUtil.parse(sql, parser -> parser.withSquareBracketQuotation(true));
                if (statement instanceof CreateTable createTable) {
                    List<String> columnNames = createTable.getColumnDefinitions().stream().map(ColumnDefinition::getColumnName).toList();
                    if (columnNames.size() != columnNames.stream().distinct().count()) {
                        throw new RuntimeException("Column names are not unique");
                    }
                    return createTable;
                } else {
                    throw new IllegalStateException("Expected CREATE TABLE, but got [" + sql + "]");
                }
            } catch (JSQLParserException e) {
                throw new RuntimeException(e);
            }
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to parse " + sql, e);
        }
    }
}
