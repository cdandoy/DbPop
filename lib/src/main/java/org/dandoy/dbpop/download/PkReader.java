package org.dandoy.dbpop.download;

import lombok.Getter;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Index;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpop.utils.DbPopUtils;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.dandoy.dbpop.utils.DbPopUtils.getPositionByColumnName;

public class PkReader {
    public static final PkInfo NO_OP_OK_INFO = new NoOpPkInfo();
    private final Map<TableName, PkInfo> pkInfoByTable = new HashMap<>();

    private PkReader() {
    }

    public static PkReader readDatasets(Database database, List<Dataset> datasets) {
        Set<TableName> tableNames = datasets.stream()
                .flatMap(dataset -> dataset.getDataFiles().stream())
                .map(DataFile::getTableName)
                .collect(Collectors.toSet());
        Collection<Table> databaseTables = database.getTables(tableNames);

        Datasets.validateAllTablesExist(datasets, tableNames, databaseTables);

        Map<TableName, Table> tablesByName = databaseTables.stream().collect(Collectors.toMap(Table::getTableName, Function.identity()));
        PkReader pkReader = new PkReader();
        for (Dataset dataset : datasets) {
            for (DataFile dataFile : dataset.getDataFiles()) {
                TableName tableName = dataFile.getTableName();
                Table table = tablesByName.get(tableName);
                pkReader.readDataFile(dataFile, table);
            }
        }
        return pkReader;
    }

    private void readDataFile(DataFile dataFile, Table table) {
        Optional<List<String>> optionalPkColumns = table.getIndexes().stream()
                .filter(Index::isPrimaryKey)
                .map(Index::getColumns)
                .findFirst();
        if (optionalPkColumns.isPresent()) {
            List<String> pkColumns = optionalPkColumns.get();
            readFile(dataFile, pkColumns);
        }
    }

    private void readFile(DataFile dataFile, List<String> pkColumns) {
        try (CSVParser csvParser = DbPopUtils.createCsvParser(dataFile.getFile())) {
            int count = 0;
            List<Integer> pkPositions = new ArrayList<>();
            for (int i = 0; i < pkColumns.size(); i++) {
                String pkColumn = pkColumns.get(i);
                if (pkColumns.contains(pkColumn)) {
                    pkPositions.add(i);
                }
            }
            TableName tableName = dataFile.getTableName();
            PkInfo pkInfo = pkInfoByTable.computeIfAbsent(tableName, it -> new PkInfoImpl(pkColumns));
            for (CSVRecord csvRecord : csvParser) {
                List<String> row = new ArrayList<>();
                try {
                    for (Integer pkPosition : pkPositions) {
                        String s = csvRecord.get(pkPosition);
                        row.add(s);
                    }
                    count++;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to process row " + count, e);
                }
                pkInfo.addRow(row);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public PkInfo getPkInfo(TableName tableName) {
        return pkInfoByTable.getOrDefault(tableName, NO_OP_OK_INFO);
    }

    interface PkInfo {
        void addRow(List<String> row);

        List<Integer> toPositions(ResultSetMetaData metaData) throws SQLException;

        boolean containsRow(List<String> row);
    }

    private static class NoOpPkInfo implements PkInfo {
        @Override
        public void addRow(List<String> row) {
        }

        @Override
        public List<Integer> toPositions(ResultSetMetaData metaData) {
            return Collections.emptyList();
        }

        @Override
        public boolean containsRow(List<String> row) {
            return false;
        }
    }

    static class PkInfoImpl implements PkInfo {
        @Getter
        private final List<String> pkColumns;
        private final Set<List<String>> pkRows = new HashSet<>();

        public PkInfoImpl(List<String> pkColumns) {
            this.pkColumns = pkColumns;
        }

        public void addRow(List<String> row) {
            pkRows.add(row);
        }

        public List<Integer> toPositions(ResultSetMetaData metaData) {
            return pkColumns.stream()
                    .map(pkColumn -> getPositionByColumnName(metaData, pkColumn))
                    .collect(Collectors.toList());
        }

        public boolean containsRow(List<String> row) {
            return pkRows.contains(row);
        }
    }
}
