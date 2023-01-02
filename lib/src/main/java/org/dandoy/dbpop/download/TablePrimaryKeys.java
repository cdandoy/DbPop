package org.dandoy.dbpop.download;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.database.PrimaryKey;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.utils.DbPopUtils;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TablePrimaryKeys {
    private final Set<List<String>> pkValues;
    private final List<SelectedColumn> selectedPkColumns;

    private TablePrimaryKeys(Set<List<String>> pkValues, List<SelectedColumn> selectedPkColumns) {
        this.pkValues = pkValues;
        this.selectedPkColumns = selectedPkColumns;
    }

    public static TablePrimaryKeys createTablePrimaryKeys(File datasetsDirectory, String dataset, Table table, List<SelectedColumn> selectedColumns) {
        PrimaryKey primaryKey = table.primaryKey();
        if (primaryKey == null) return null;
        Set<List<String>> pkValues = new HashSet<>();
        List<SelectedColumn> selectedPkColumns = primaryKey.columns().stream()
                .map(columnName -> SelectedColumn.findByName(selectedColumns, columnName))
                .toList();

        readPrimaryKeys(pkValues, datasetsDirectory, table, Datasets.STATIC);
        readPrimaryKeys(pkValues, datasetsDirectory, table, Datasets.BASE);
        readPrimaryKeys(pkValues, datasetsDirectory, table, dataset);
        return new TablePrimaryKeys(pkValues, selectedPkColumns);
    }

    private static void readPrimaryKeys(Set<List<String>> pkValues, File datasetsDirectory, Table table, String dataset) {
        File outputFile = DbPopUtils.getOutputFile(datasetsDirectory, dataset, table.tableName());
        if (outputFile.exists()) {
            try (CSVParser csvParser = DbPopUtils.createCsvParser(outputFile)) {
                PrimaryKey primaryKey = table.primaryKey();
                List<String> columns = primaryKey.columns();
                for (CSVRecord csvRecord : csvParser) {
                    List<String> values = new ArrayList<>();
                    for (String column : columns) {
                        String value = csvRecord.get(column);
                        values.add(value);
                    }
                    pkValues.add(values);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read " + outputFile);
            }
        }
    }

    public boolean addPrimaryKey(ResultSet resultSet) {
        List<String> pkRow = selectedPkColumns.stream().map(selectedColumn -> toPkString(resultSet, selectedColumn)).toList();
        return pkValues.add(pkRow);
    }

    private static String toPkString(ResultSet resultSet, SelectedColumn selectedPkColumn) {
        try {
            return resultSet.getString(selectedPkColumn.jdbcPos());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read " + selectedPkColumn.name(), e);
        }
    }
}
