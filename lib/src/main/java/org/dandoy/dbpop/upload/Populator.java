package org.dandoy.dbpop.upload;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.database.Database.DatabaseInserter;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.utils.AutoComitterOff;
import org.dandoy.dbpop.utils.DbPopUtils;
import org.dandoy.dbpop.utils.StopWatch;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class Populator implements AutoCloseable {
    private static Populator INSTANCE;
    private final ConnectionBuilder connectionBuilder;
    private final Database database;
    @Getter
    private final Map<String, Dataset> datasetsByName;
    @Getter
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
            // Creates a default Populator based on the properties found in ~/dbpop.properties
            INSTANCE = builder().build();
        }
        return INSTANCE;
    }

    static synchronized void setInstance(Populator instance) {
        if (INSTANCE instanceof CloseShieldPopulator closeShieldPopulator) {
            closeShieldPopulator.doClose();
        }
        INSTANCE = instance;
    }

    private static Populator build(Builder builder, boolean closeShield) {
        try {
            Database database = Database.createDatabase(builder.getConnectionBuilder().createConnection());
            File directory = builder.getDirectory();
            List<Dataset> allDatasets = Datasets.getDatasets(directory);
            if (allDatasets.isEmpty()) throw new RuntimeException("No datasets found in " + directory);

            Set<TableName> datasetTableNames = allDatasets.stream()
                    .flatMap(dataset -> dataset.getDataFiles().stream())
                    .map(DataFile::getTableName)
                    .collect(Collectors.toSet());

            Collection<Table> databaseTables = database.getTables(datasetTableNames);
            Datasets.validateAllTablesExist(allDatasets, datasetTableNames, databaseTables);

            Map<String, Dataset> datasetsByName = allDatasets.stream().collect(Collectors.toMap(Dataset::getName, Function.identity()));
            Map<TableName, Table> tablesByName = databaseTables.stream().collect(Collectors.toMap(Table::tableName, Function.identity()));
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
        if (!datasetsByName.containsKey(Datasets.STATIC)) return datasets;
        ArrayList<String> ret = new ArrayList<>(datasets);

        if (ret.remove(Datasets.STATIC))
            log.warn("Cannot explicitely load the static dataset");

        if (staticLoaded++ == 0)
            ret.add(0, Datasets.STATIC);

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
                throw new RuntimeException(message, e);
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
    @Getter
    @Setter
    @Accessors(chain = true)
    public static class Builder {
        private String dbUrl;
        private String dbUser;
        private String dbPassword;
        private File directory;

        private Builder() {
        }

        public ConnectionBuilder getConnectionBuilder() {
            return new UrlConnectionBuilder(
                    getDbUrl(),
                    getDbUser(),
                    getDbPassword()
            );
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

        File getDirectory() {
            return directory;
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
