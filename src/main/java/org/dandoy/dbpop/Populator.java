package org.dandoy.dbpop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.Database.DatabaseInserter;

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
    private boolean verbose;

    public Populator(Database database, List<Dataset> datasets, Collection<Table> tables) {
        this.database = database;
        this.datasetsByName = datasets.stream().collect(Collectors.toMap(Dataset::getName, Function.identity()));
        this.tablesByName = tables.stream().collect(Collectors.toMap(Table::getTableName, Function.identity()));
    }

    @Override
    public void close() {
    }

    public int load(List<String> datasets) {
        int rowCount = 0;
        Set<Table> loadedTables = getLoadedTables(datasets);
        try (DatabasePreparationStrategy ignored = DatabasePreparationStrategy.createDatabasePreparationStrategy(database, tablesByName, loadedTables)) {
            try {
                database.connection.setAutoCommit(false);
                try {
                    for (String datasetName : datasets) {
                        Dataset dataset = datasetsByName.get(datasetName);
                        rowCount += loadDataset(dataset);
                    }
                } finally {
                    database.connection.setAutoCommit(true);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return rowCount;
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
        if (verbose) {
            System.out.printf("Loading %s%n", tableName.toQualifiedName());
        }
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

    public Populator setVerbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }
}
