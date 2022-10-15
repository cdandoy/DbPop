package org.dandoy.dbpop.upload;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.database.Database.DatabaseInserter;
import org.dandoy.dbpop.fs.LocalFileSystem;
import org.dandoy.dbpop.fs.SimpleFileSystem;
import org.dandoy.dbpop.utils.AutoComitterOff;
import org.dandoy.dbpop.utils.StopWatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class Populator implements AutoCloseable {
    public static final String STATIC = "static";
    private static Populator INSTANCE;
    private final ConnectionBuilder connectionBuilder;
    private final Database database;
    private final Map<String, Dataset> datasetsByName;
    private final Map<TableName, Table> tablesByName;
    private int staticLoaded;

    protected Populator(ConnectionBuilder connectionBuilder, Database database, Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName) {
        this.connectionBuilder = connectionBuilder;
        this.database = database;
        this.datasetsByName = datasetsByName;
        this.tablesByName = tablesByName;
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

    public static synchronized Populator getInstance() {
        if (INSTANCE == null) {
            INSTANCE = build();
        }
        return INSTANCE;
    }

    static synchronized void setInstance(Populator instance) {
        if (INSTANCE instanceof CloseShieldPopulator) {
            CloseShieldPopulator closeShieldPopulator = (CloseShieldPopulator) INSTANCE;
            closeShieldPopulator.doClose();
        }
        INSTANCE = instance;
    }

    /**
     * Creates a default Populator based on the properties found in ~/dbpop.properties
     *
     * @return a default Populator
     */
    @SuppressWarnings("unused")
    public static Populator build() {
        return builder().build();
    }

    private static Populator build(Builder builder, boolean closeShield) {
        try {
            Database database = Database.createDatabase(builder.getConnectionBuilder().createConnection());
            List<Dataset> allDatasets = getDatasets(builder.getSimpleFileSystem());
            if (allDatasets.isEmpty()) throw new RuntimeException("No datasets found in " + builder.getSimpleFileSystem());

            Set<TableName> datasetTableNames = allDatasets.stream()
                    .flatMap(dataset -> dataset.getDataFiles().stream())
                    .map(DataFile::getTableName)
                    .collect(Collectors.toSet());

            Collection<Table> databaseTables = database.getTables(datasetTableNames);
            validateAllTablesExist(allDatasets, datasetTableNames, databaseTables);

            Map<String, Dataset> datasetsByName = allDatasets.stream().collect(Collectors.toMap(Dataset::getName, Function.identity()));
            Map<TableName, Table> tablesByName = databaseTables.stream().collect(Collectors.toMap(Table::getTableName, Function.identity()));
            validateStaticTables(datasetsByName);

            if (closeShield) {
                return new CloseShieldPopulator(builder.getConnectionBuilder(), database, datasetsByName, tablesByName);
            } else {
                return new Populator(builder.getConnectionBuilder(), database, datasetsByName, tablesByName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Validate that none of the tables in the static dataset are also in another dataset
     */
    private static void validateStaticTables(Map<String, Dataset> datasetsByName) {
        Dataset staticDataset = datasetsByName.get(STATIC);
        if (staticDataset == null) return;
        Set<TableName> staticTableNames = staticDataset.getDataFiles().stream().map(DataFile::getTableName).collect(Collectors.toSet());
        for (Map.Entry<String, Dataset> datasetEntry : datasetsByName.entrySet()) {
            String datasetName = datasetEntry.getKey();
            if (!STATIC.equals(datasetName)) {
                Dataset dataset = datasetEntry.getValue();
                for (DataFile dataFile : dataset.getDataFiles()) {
                    TableName tableName = dataFile.getTableName();
                    if (staticTableNames.contains(tableName)) {
                        throw new RuntimeException("Table cannot be both in the static and in the " + datasetName + " datasets");
                    }
                }
            }
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
                    badDataFile.getSimpleFileSystem()
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
        return StopWatch.record("Populator.load()", () -> {
            List<String> adjustedDatasets = adjustDatasetsForStatic(datasets);
            log.debug("---- Loading {}", String.join(", ", adjustedDatasets));
            try (AutoComitterOff ignored = new AutoComitterOff(database.getConnection())) {
                int rowCount = 0;

                DatabasePreparationStrategy databasePreparationStrategy = database.createDatabasePreparationStrategy(datasetsByName, tablesByName, adjustedDatasets);
                databasePreparationStrategy.beforeInserts();
                try {
                    for (String datasetName : adjustedDatasets) {
                        Dataset dataset = datasetsByName.get(datasetName);
                        if (dataset == null) throw new RuntimeException("Dataset not found: " + datasetName);
                        rowCount += loadDataset(dataset);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
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
    private List<String> adjustDatasetsForStatic(List<String> datasets) {
        // No need to adjust if we don't have a static dataset
        if (!datasetsByName.containsKey(STATIC)) return datasets;
        ArrayList<String> ret = new ArrayList<>(datasets);

        if (ret.remove(STATIC))
            log.warn("Cannot explicitely load the static dataset");

        if (staticLoaded++ == 0)
            ret.add(0, STATIC);

        return ret;
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
        return StopWatch.record("loadDataFile", () -> {
            TableName tableName = dataFile.getTableName();
            log.debug(String.format("Loading %-60s", tableName.toQualifiedName()));
            try {
                Table table = tablesByName.get(tableName);
                CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                        .setHeader()
                        .setSkipHeaderRecord(true)
                        .setNullString("")
                        .build();
                try (CSVParser csvParser = csvFormat.parse(new BufferedReader(new InputStreamReader(dataFile.createInputStream(), StandardCharsets.UTF_8)))) {
                    return insertRows(table, csvParser);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to load " + tableName.toQualifiedName(), e);
            }
        });
    }

    private int insertRows(Table table, CSVParser csvParser) {
        int count = 0;
        List<String> headerNames = csvParser.getHeaderNames();
        List<DataFileHeader> dataFileHeaders = headerNames.stream().map(DataFileHeader::new).collect(Collectors.toList());
        try (DatabaseInserter databaseInserter = database.createInserter(table, dataFileHeaders)) {
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

    private static List<Dataset> getDatasets(SimpleFileSystem simpleFileSystem) {
        return StopWatch.record("Populator.getDatasets", () -> {
            List<Dataset> datasets = new ArrayList<>();
            Collection<SimpleFileSystem> datasetFiles = simpleFileSystem.list();
            if (datasetFiles.isEmpty()) throw new RuntimeException("Invalid path " + simpleFileSystem);
            for (SimpleFileSystem datasetFile : datasetFiles) {
                Collection<SimpleFileSystem> catalogFiles = datasetFile.list();
                Collection<DataFile> dataFiles = new ArrayList<>();
                for (SimpleFileSystem catalogFile : catalogFiles) {
                    String catalog = catalogFile.getName();
                    Collection<SimpleFileSystem> schemaFiles = catalogFile.list();
                    for (SimpleFileSystem schemaFile : schemaFiles) {
                        String schema = schemaFile.getName();
                        Collection<SimpleFileSystem> tableFiles = schemaFile.list();
                        for (SimpleFileSystem tableFile : tableFiles) {
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
                if (!dataFiles.isEmpty()) {
                    datasets.add(
                            new Dataset(
                                    datasetFile.getName(),
                                    dataFiles
                            )
                    );
                }
            }
            return datasets;
        });
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
        private String path;
        private File directory;

        /**
         * For example:
         * <pre>
         * path/
         *   base/
         *     AdventureWorks/
         *       HumanResources/
         *         Department.csv
         *         Employee.csv
         *         Shift.csv
         * </pre>
         *
         * @param path The path that holds the datasets
         * @return this
         */
        public Builder setPath(String path) {
            this.path = path;
            return this;
        }

        public Builder setDirectory(String directory) {
            return setDirectory(new File(directory));
        }

        public Builder setDirectory(File directory) {
            if (directory != null) {
                if (!directory.isDirectory()) {
                    throw new RuntimeException("Invalid directory: " + directory.getAbsolutePath());
                }
            }
            this.directory = directory;
            return this;
        }

        SimpleFileSystem getSimpleFileSystem() {
            if (directory != null) {
                return new LocalFileSystem(directory, path == null ? "/" : path);
            }
            if (path != null) {
                return SimpleFileSystem.fromPath(path);
            }
            return SimpleFileSystem.fromPath("/testdata/");
        }

        /**
         * Builds the Populator
         *
         * @return the Populator
         */
        public Populator build() {
            return Populator.build(this, false);
        }

        public synchronized void createSingletonInstance() {
            setInstance(Populator.build(this, true));
        }
    }
}
