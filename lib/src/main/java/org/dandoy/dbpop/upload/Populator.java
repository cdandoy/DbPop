package org.dandoy.dbpop.upload;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.utils.AutoComitterOff;
import org.dandoy.dbpop.utils.DbPopUtils;
import org.dandoy.dbpop.utils.MultiCauseException;
import org.dandoy.dbpop.utils.StopWatch;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class Populator {
    private final Database database;
    @Getter
    private final Map<String, Dataset> datasetsByName;
    @Getter
    private final Map<TableName, Table> tablesByName;
    @Getter
    @Setter
    private boolean staticLoaded;

    protected Populator(Database database, Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName) {
        this.database = database;
        this.datasetsByName = datasetsByName;
        this.tablesByName = tablesByName;
    }

    public static Populator createPopulator(Database database, File directory) {
        List<Dataset> allDatasets = Datasets.getDatasets(directory);
        if (allDatasets.isEmpty()) throw new RuntimeException("No datasets found in " + directory);

        Set<TableName> datasetTableNames = allDatasets.stream()
                .flatMap(dataset -> dataset.getDataFiles().stream())
                .map(DataFile::getTableName)
                .collect(Collectors.toSet());

        Collection<Table> databaseTables = database.getTables(datasetTableNames);
        Datasets.validateAllTablesExist(allDatasets, datasetTableNames, databaseTables);

        Map<String, Dataset> datasetsByName = allDatasets.stream().collect(Collectors.toMap(Dataset::getName, Function.identity()));
        Map<TableName, Table> tablesByName = databaseTables.stream().collect(Collectors.toMap(Table::getTableName, Function.identity()));
        validateStaticTables(datasetsByName);

        return new Populator(database, datasetsByName, tablesByName);
    }

    /**
     * Validate that none of the tables in the static dataset are also in another dataset
     */
    private static void validateStaticTables(Map<String, Dataset> datasetsByName) {
        Dataset staticDataset = datasetsByName.get(Datasets.STATIC);
        if (staticDataset == null) return;
        Set<TableName> staticTableNames = staticDataset.getDataFiles().stream().map(DataFile::getTableName).collect(Collectors.toSet());
        for (Map.Entry<String, Dataset> datasetEntry : datasetsByName.entrySet()) {
            String datasetName = datasetEntry.getKey();
            if (!Datasets.STATIC.equals(datasetName)) {
                Dataset dataset = datasetEntry.getValue();
                for (DataFile dataFile : dataset.getDataFiles()) {
                    TableName tableName = dataFile.getTableName();
                    if (staticTableNames.contains(tableName)) {
                        throw new RuntimeException(String.format(
                                "Table %s cannot be both in the static and in the %s datasets",
                                tableName.toQualifiedName(),
                                datasetName
                        ));
                    }
                }
            }
        }
    }

    /**
     * Loads the datasets.
     *
     * @param datasets the datasets to load
     * @return the number of rows loaded
     */
    @SuppressWarnings("UnusedReturnValue")
    public int load(String... datasets) {
        return load(Arrays.asList(datasets));
    }

    /**
     * Loads the datasets.
     *
     * @param datasets the datasets to load
     * @return the number of rows loaded
     */
    public int load(List<String> datasets) {
        return StopWatch.record("Populator.load()", () -> {
            List<String> adjustedDatasets = adjustDatasets(datasets);
            log.debug("---- Loading {}", String.join(", ", adjustedDatasets));
            try (AutoComitterOff ignored = new AutoComitterOff(database.getConnection())) {
                int rowCount = 0;

                Set<TableName> allTables = datasetsByName.values().stream()
                        .flatMap(it -> it.getDataFiles().stream())
                        .map(DataFile::getTableName)
                        .collect(Collectors.toSet());
                if (!adjustedDatasets.contains(Datasets.STATIC)) {
                    // exclude the static tables if we don't reload it
                    Dataset dataset = datasetsByName.get(Datasets.STATIC);
                    if (dataset != null) {
                        dataset
                                .getDataFiles().stream()
                                .map(DataFile::getTableName)
                                .forEach(allTables::remove);
                    }
                }

                DatabasePreparationStrategy databasePreparationStrategy = database.createDatabasePreparationFactory()
                        .createDatabasePreparationStrategy(
                                database,
                                allTables
                        );

                databasePreparationStrategy.beforeInserts();
                try {
                    for (String datasetName : adjustedDatasets) {
                        Dataset dataset = datasetsByName.get(datasetName);
                        if (dataset == null) throw new RuntimeException("Dataset not found: " + datasetName);
                        rowCount += loadDataset(dataset);
                        if (datasetName.equals(Datasets.STATIC)) {
                            staticLoaded = true;
                        }
                    }
                } finally {
                    databasePreparationStrategy.afterInserts();
                }
                return rowCount;
            }
        });
    }

    /**
     * The static dataset is only loaded once per Populator
     */
    private List<String> adjustDatasets(List<String> datasets) {
        // We move the datasets to 0, so do in reverse order
        List<String> ret = new ArrayList<>(datasets);
        if (datasetsByName.containsKey(Datasets.BASE)) {    // No need to adjust if we don't have a base dataset
            ret.remove(Datasets.BASE);
            ret.add(0, Datasets.BASE);
        }

        if (datasetsByName.containsKey(Datasets.STATIC)) {  // No need to adjust if we don't have a static dataset
            ret.remove(Datasets.STATIC);
            if (!staticLoaded) {
                ret.add(0, Datasets.STATIC);
            }
        }

        return ret;
    }

    private int loadDataset(Dataset dataset) {
        int rowCount = 0;
        try {
            for (DataFile dataFile : dataset.getDataFiles()) {
                rowCount += loadDataFile(dataFile);
            }
        } catch (Exception e) {
            throw new PopulateDatasetException(dataset.getName(), String.format("Failed to load the dataset %s", dataset.getName()), e);
        }
        return rowCount;
    }

    private int loadDataFile(DataFile dataFile) {
        return StopWatch.record("loadDataFile", () -> {
            File file = dataFile.getFile();
            TableName tableName = dataFile.getTableName();
            log.debug(String.format("Loading %-60s", tableName.toQualifiedName()));
            try {
                Table table = tablesByName.get(tableName);
                try (CSVParser csvParser = DbPopUtils.createCsvParser(file)) {
                    return insertRows(table, csvParser);
                }
            } catch (Exception e) {
                String message = String.format(
                        "Failed to load %s from %s",
                        tableName.toQualifiedName(),
                        dataFile.getFile()
                );
                throw new MultiCauseException(message, e);
            }
        });
    }

    private int insertRows(Table table, CSVParser csvParser) {
        int count = 0;
        List<String> headerNames = csvParser.getHeaderNames();
        List<DataFileHeader> dataFileHeaders = headerNames.stream().map(DataFileHeader::new).collect(Collectors.toList());
        try (DefaultDatabase.DatabaseInserter databaseInserter = database.createInserter(table, dataFileHeaders)) {
            for (CSVRecord csvRecord : csvParser) {
                try {
                    databaseInserter.insert(csvRecord);
                    count++;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to process row " + count, e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return count;
    }
}
