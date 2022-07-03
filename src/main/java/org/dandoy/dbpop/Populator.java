package org.dandoy.dbpop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.Database.DatabaseInserter;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Populator implements AutoCloseable {
    private final ConnectionBuilder connectionBuilder;
    private final Database database;
    private final Map<String, Dataset> datasetsByName;
    private final Map<TableName, Table> tablesByName;
    private final boolean verbose;

    private Populator(ConnectionBuilder connectionBuilder, Database database, Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, boolean verbose) {
        this.connectionBuilder = connectionBuilder;
        this.database = database;
        this.datasetsByName = datasetsByName;
        this.tablesByName = tablesByName;
        this.verbose = verbose;
    }

    /**
     * Populators are created using a builder pattern.
     * Example:
     * <pre>
     *     Populator.builder()
     *                 .setDirectory("src/test/resources/tests")
     *                 .setConnection("jdbc:sqlserver://localhost", "sa", "****")
     *                 .setVerbose(true)
     *                 .build()
     * </pre>
     *
     * @return a Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a default Populator based on the properties found in ~/dbpop.properties
     *
     * @return a default Populator
     */
    public static Populator build() {
        return builder().build();
    }

    private static Populator build(Builder builder) {
        try {
            Database database = Database.createDatabase(builder.getConnectionBuilder().createConnection());
            List<Dataset> allDatasets = getDatasets(builder.getDirectory());
            if (allDatasets.isEmpty()) throw new RuntimeException("No datasets found in " + builder.getDirectory());

            Set<TableName> datasetTableNames = allDatasets.stream()
                    .flatMap(dataset -> dataset.getDataFiles().stream())
                    .map(DataFile::getTableName)
                    .collect(Collectors.toSet());

            Collection<Table> databaseTables = database.getTables(datasetTableNames);
            validateAllTablesExist(allDatasets, datasetTableNames, databaseTables);

            Map<String, Dataset> datasetsByName = allDatasets.stream().collect(Collectors.toMap(Dataset::getName, Function.identity()));
            Map<TableName, Table> tablesByName = databaseTables.stream().collect(Collectors.toMap(Table::getTableName, Function.identity()));
            return new Populator(builder.getConnectionBuilder(), database, datasetsByName, tablesByName, builder.isVerbose());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Check that we have all the tables that are in the dataset
     *
     * @param allDatasets       The datasets
     * @param datasetTableNames The table names found in the data sets
     * @param databaseTables    the tables found in the database that are in the data sets
     */
    private static void validateAllTablesExist(List<Dataset> allDatasets, Set<TableName> datasetTableNames, Collection<Table> databaseTables) {
        Set<TableName> databaseTableNames = databaseTables.stream().map(Table::getTableName).collect(Collectors.toSet());
        List<TableName> missingTables = datasetTableNames.stream()
                .filter(tableName -> !databaseTableNames.contains(tableName))
                .collect(Collectors.toList());
        if (!missingTables.isEmpty()) {
            DataFile badDataFile = allDatasets.stream()
                    .flatMap(dataset -> dataset.getDataFiles().stream())
                    .filter(dataFile -> missingTables.contains(dataFile.getTableName()))
                    .findFirst()
                    .orElseThrow(RuntimeException::new);
            throw new RuntimeException(String.format(
                    "Table %s does not exist for this data file %s",
                    badDataFile.getTableName().toQualifiedName(),
                    badDataFile.getFile()
            ));
        }
    }

    @Override
    public void close() {
        database.close();
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
        if (verbose) System.out.println("---- Loading " + String.join(", ", datasets));

        int rowCount = 0;
        Set<Table> loadedTables = getLoadedTables(datasets);

        try (DatabasePreparationStrategy ignored = database.createDatabasePreparationStrategy(tablesByName, loadedTables)) {
            try {
                database.connection.setAutoCommit(false);
                try {
                    for (String datasetName : datasets) {
                        Dataset dataset = datasetsByName.get(datasetName);
                        if (dataset == null) throw new RuntimeException("Dataset not found: " + datasetName);
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
        long t0 = System.currentTimeMillis();
        if (verbose) {
            System.out.printf("Loading %-60s", tableName.toQualifiedName());
            System.out.flush();
        }
        try {
            Table table = tablesByName.get(tableName);
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setNullString("")
                    .build();
            try (CSVParser csvParser = csvFormat.parse(Files.newBufferedReader(dataFile.getFile().toPath(), StandardCharsets.UTF_8))) {
                int rows = insertRows(table, csvParser);
                if (verbose) {
                    long t1 = System.currentTimeMillis();
                    System.out.printf(" %5d rows %4dms%n", rows, t1 - t0);
                }
                return rows;
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

    private static List<Dataset> getDatasets(File directory) {
        List<Dataset> datasets = new ArrayList<>();
        File[] datasetFiles = directory.listFiles();
        if (datasetFiles == null) throw new RuntimeException("Invalid directory " + directory);
        for (File datasetFile : datasetFiles) {
            File[] catalogFiles = datasetFile.listFiles();
            if (catalogFiles != null) {
                Collection<DataFile> dataFiles = new ArrayList<>();
                for (File catalogFile : catalogFiles) {
                    String catalog = catalogFile.getName();
                    File[] schemaFiles = catalogFile.listFiles();
                    if (schemaFiles != null) {
                        for (File schemaFile : schemaFiles) {
                            String schema = schemaFile.getName();
                            File[] tableFiles = schemaFile.listFiles();
                            if (tableFiles != null) {
                                for (File tableFile : tableFiles) {
                                    String tableFileName = tableFile.getName();
                                    if (tableFileName.endsWith(".csv")) {
                                        String table = tableFileName.substring(0, tableFileName.length() - 4);
                                        dataFiles.add(
                                                new DataFile(
                                                        tableFile,
                                                        new TableName(catalog, schema, table)
                                                )
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                if (!dataFiles.isEmpty()) {
                    datasets.add(
                            new Dataset(
                                    datasetFile.getName(),
                                    dataFiles
                            )
                    );
                }
            }
        }
        return datasets;
    }

    /**
     * @return a connection to the test database.
     * @throws SQLException An exception that provides information on a database access error or other errors.
     */
    public Connection createConnection() throws SQLException {
        return connectionBuilder.createConnection();
    }

    /**
     * Populator builder
     */
    public static class Builder extends DefaultBuilder<Builder, Populator> {

        /**
         * Builds the Populator
         *
         * @return the Populator
         */
        public Populator build() {
            super.validate();
            return Populator.build(this);
        }
    }
}
