package org.dandoy;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.Database.DatabaseInserter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Populator implements AutoCloseable {
    private final Database database;
    private final Map<String, Dataset> datasetsByName;
    private final Map<TableName, Table> tablesByName;

    public Populator(Database database, List<Dataset> datasets, Collection<Table> tables) {
        this.database = database;
        this.datasetsByName = datasets.stream().collect(Collectors.toMap(Dataset::getName, Function.identity()));
        this.tablesByName = tables.stream().collect(Collectors.toMap(Table::getTableName, Function.identity()));
    }

    @Override
    public void close() {
    }

    public int load(List<String> datasets) {
        Set<Table> loadedTables = getLoadedTables(datasets);
        Set<ForeignKey> affectedForeignKeys = getAffectedForeignKeys(loadedTables);
        Set<Index> affectedIndexes = getAffectedIndexes(loadedTables);
        dropForeignKeys(affectedForeignKeys);
        dropIndexes(affectedIndexes);

        truncateTables(loadedTables);

        try {
            int rowCount = 0;
            database.connection.setAutoCommit(false);
            try {
                for (String datasetName : datasets) {
                    Dataset dataset = datasetsByName.get(datasetName);
                    rowCount += loadDataset(dataset);
                }
            } finally {
                database.connection.setAutoCommit(true);
            }
            createIndexes(affectedIndexes);
            createForeignKeys(affectedForeignKeys);
            return rowCount;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Set<Table> getLoadedTables(List<String> datasets) {
        return this.datasetsByName
                .values().stream()
                .filter(it -> datasets.contains(it.getName()))
                .map(Dataset::getDataFiles)
                .flatMap(Collection::stream)
                .map(it -> tablesByName.get(it.getTableName()))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    private Set<ForeignKey> getAffectedForeignKeys(Set<Table> tables) {
        return tables.stream()
                .map(Table::getTableName)
                .map(tablesByName::get)
                .filter(Objects::nonNull)
                .flatMap(table -> table.getForeignKeys().stream())
                .collect(Collectors.toSet());
    }

    private Set<Index> getAffectedIndexes(Set<Table> tables) {
        return tables.stream()
                .map(Table::getIndexes)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private void dropForeignKeys(Set<ForeignKey> foreignKeys) {
        foreignKeys.forEach(database::dropForeignKey);
    }

    private void dropIndexes(Set<Index> indexes) {
        indexes.forEach(database::dropIndex);
    }

    private void createIndexes(Set<Index> indexes) {
        indexes.forEach(database::createIndex);
    }

    private void createForeignKeys(Set<ForeignKey> foreignKeys) {
        foreignKeys.forEach(database::createForeignKey);
    }

    private void truncateTables(Set<Table> tables) {
        tables.forEach(database::truncateTable);
    }

    private int loadDataset(Dataset dataset) {
        int rowCount = 0;
        try {
            for (DataFile dataFile : dataset.getDataFiles()) {
                rowCount += loadDataFile(dataFile);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to load the dataset %s", dataset.getName()), e);
        }
        return rowCount;
    }

    private int loadDataFile(DataFile dataFile) {
        TableName tableName = dataFile.getTableName();
        System.out.printf("Loading %s%n", tableName.toQualifiedName());
        try {
            Table table = tablesByName.get(tableName);
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setNullString("")
                    .build();
            try (CSVParser csvParser = csvFormat.parse(Files.newBufferedReader(dataFile.getFile().toPath(), StandardCharsets.UTF_8))) {
                return insertRows(table, csvParser);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load " + tableName.toQualifiedName(), e);
        }
    }

    private int insertRows(Table table, CSVParser csvParser) {
        int count = 0;
        List<String> headerNames = csvParser.getHeaderNames();
        try (DatabaseInserter databaseInserter = database.createInserter(table, headerNames)) {
            for (CSVRecord csvRecord : csvParser) {
                databaseInserter.insert(csvRecord);
                count++;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return count;
    }
}
