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
        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey == null) return null;
        Set<List<String>> pkValues = new HashSet<>();
        List<SelectedColumn> selectedPkColumns = primaryKey.getColumns().stream()
                .map(columnName -> SelectedColumn.findByName(selectedColumns, columnName))
                .toList();

        readPrimaryKeys(pkValues, datasetsDirectory, table, Datasets.STATIC);
        readPrimaryKeys(pkValues, datasetsDirectory, table, Datasets.BASE);
        readPrimaryKeys(pkValues, datasetsDirectory, table, dataset);
        return new TablePrimaryKeys(pkValues, selectedPkColumns);
    }

    private static void readPrimaryKeys(Set<List<String>> pkValues, File datasetsDirectory, Table table, String dataset) {
        File outputFile = DbPopUtils.getOutputFile(datasetsDirectory, dataset, table.getTableName());
        if (outputFile.exists()) {
            try (CSVParser csvParser = DbPopUtils.createCsvParser(outputFile)) {
                PrimaryKey primaryKey = table.getPrimaryKey();
                List<String> columns = primaryKey.getColumns();
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

    public List<String> extractPrimaryKey(ResultSet resultSet) {
        List<String> ret = new ArrayList<>();
        for (SelectedColumn selectedPkColumn : selectedPkColumns) {
            try {
                String s = resultSet.getString(selectedPkColumn.jdbcPos());
                ret.add(s);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to read " + selectedPkColumn.name(), e);
            }
        }
        return ret;
    }

    public boolean addPrimaryKey(List<String> pkRow) {
        return pkValues.add(pkRow);
    }
}
