package org.dandoy.dbpop.download;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.utils.DbPopUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record TableExpressions(Map<List<String>, RowExpressions> expressionsByPk) {
    public static class RowExpressions {
        private final Map<String, String> expressionsByColumnName = new HashMap<>();

        void addExpression(String columnName, String expression) {
            expressionsByColumnName.put(columnName, expression);
        }

        public String getExpression(String columnName) {
            return expressionsByColumnName.get(columnName);
        }
    }

    public RowExpressions getColumnExpressions(List<String> pkRow) {
        return expressionsByPk.get(pkRow);
    }

    public static TableExpressions createTableExpressions(Database database, DataFile dataFile) {
        Map<List<String>, RowExpressions> expressionsByPk = null;
        List<String> pkColumns = getPkColumns(database, dataFile.getTableName());
        if (pkColumns != null) {
            try (CSVParser csvParser = DbPopUtils.createCsvParser(dataFile.getFile())) {
                List<String> headerNames = csvParser.getHeaderNames();
                ExpressionParser expressionParser = new ExpressionParser();
                List<Integer> pkPositions = getPkPositions(csvParser, pkColumns);
                for (CSVRecord csvRecord : csvParser) {
                    for (int i = 0; i < csvRecord.size(); i++) {
                        String s = csvRecord.get(i);
                        Object evaluated = expressionParser.evaluate(s);
                        if (evaluated != s) {
                            List<String> pk = createPk(csvRecord, pkPositions);
                            String columnName = headerNames.get(i);
                            if (expressionsByPk == null) {
                                expressionsByPk = new HashMap<>();
                            }
                            expressionsByPk.computeIfAbsent(pk, strings -> new RowExpressions())
                                    .addExpression(columnName, s);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (expressionsByPk != null) {
            return new TableExpressions(expressionsByPk);
        }
        return null;
    }

    private static List<String> createPk(CSVRecord csvRecord, List<Integer> pkPositions) {
        List<String> ret = new ArrayList<>(pkPositions.size());
        for (Integer pkPosition : pkPositions) {
            String pk = csvRecord.get(pkPosition);
            ret.add(pk);
        }
        return ret;
    }

    private static List<Integer> getPkPositions(CSVParser csvParser, List<String> pkColumns) {
        List<Integer> ret = new ArrayList<>(pkColumns.size());
        List<String> headerNames = csvParser.getHeaderNames();
        for (String pkColumn : pkColumns) {
            for (int i = 0; i < headerNames.size(); i++) {
                String headerName = headerNames.get(i);
                if (pkColumn.equals(headerName)) {
                    ret.add(i);
                }
            }
        }
        return ret;
    }


    private static List<String> getPkColumns(Database database, TableName tableName) {
        Table table = database.getTable(tableName);
        if (table != null) {
            PrimaryKey primaryKey = table.getPrimaryKey();
            if (primaryKey != null) {
                return primaryKey.getColumns();
            }
        }
        return null;
    }
}
