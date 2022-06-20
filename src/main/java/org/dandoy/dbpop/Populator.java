package org.dandoy.dbpop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.Database.DatabaseInserter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
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
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a default Populator based on the properties found in ~/dbpop.properties
     */
    public static Populator build() {
        return builder().build();
    }

    private static Populator build(Builder builder) {
        try {
            Database database = Database.createDatabase(builder.connectionBuilder.createConnection());
            List<Dataset> allDatasets = getDatasets(builder.directory);
            if (allDatasets.isEmpty()) throw new RuntimeException("No datasets found in " + builder.directory);

            Set<TableName> datasetTableNames = allDatasets.stream()
                    .flatMap(dataset -> dataset.getDataFiles().stream())
                    .map(DataFile::getTableName)
                    .collect(Collectors.toSet());

            Collection<Table> databaseTables = database.getTables(datasetTableNames);
            validateAllTablesExist(allDatasets, datasetTableNames, databaseTables);

            Map<String, Dataset> datasetsByName = allDatasets.stream().collect(Collectors.toMap(Dataset::getName, Function.identity()));
            Map<TableName, Table> tablesByName = databaseTables.stream().collect(Collectors.toMap(Table::getTableName, Function.identity()));
            return new Populator(builder.connectionBuilder, database, datasetsByName, tablesByName, builder.verbose);
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
     * Loads those datasets.
     */
    @SuppressWarnings("UnusedReturnValue")
    public int load(String... datasets) {
        return load(Arrays.asList(datasets));
    }

    /**
     * Loads those datasets.
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
     * Creates a connection to the test database.
     */
    public Connection createConnection() throws SQLException {
        return connectionBuilder.createConnection();
    }

    /**
     * Populator builder
     */
    public static class Builder {
        private ConnectionBuilder connectionBuilder;
        private File directory;
        private boolean verbose;

        public Builder() {
        }

        private Builder setConnectionBuilder(ConnectionBuilder connectionBuilder) {
            this.connectionBuilder = connectionBuilder;
            return this;
        }

        /**
         * How to connect to the database.
         */
        public Builder setConnection(String dbUrl, String dbUser, String dbPassword) {
            return setConnectionBuilder(new UrlConnectionBuilder(dbUrl, dbUser, dbPassword));
        }

        /**
         * The directory that holds the datasets.
         * For example:
         * <pre>
         * directory/
         *   base/
         *     AdventureWorks/
         *       HumanResources/
         *         Department.csv
         *         Employee.csv
         *         Shift.csv
         * </pre>
         */
        public Builder setDirectory(File directory) {
            try {
                if (directory == null) throw new RuntimeException("Directory cannot be null");
                this.directory = directory.getAbsoluteFile().getCanonicalFile();
                if (!this.directory.isDirectory()) throw new RuntimeException("Invalid dataset directory: " + this.directory);
                return this;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * The directory that holds the datasets.
         *
         * @see #setDirectory(File)
         */
        public Builder setDirectory(String path) {
            return setDirectory(new File(path));
        }

        /**
         * Enables verbose logging on System.out
         */
        public Builder setVerbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        /**
         * Builds the Populator
         */
        public Populator build() {
            if (directory == null) findDirectory();
            if (directory == null) throw new RuntimeException("You must specify a dataset directory");
            if (!directory.isDirectory()) throw new RuntimeException("Invalid directory: " + directory);

            if (connectionBuilder == null) setupConnectionFromExternal();
            if (connectionBuilder == null) throw new RuntimeException("You must specify the database connection");
            try (Connection connection = connectionBuilder.createConnection()) {
                connection.getMetaData();
            } catch (SQLException e) {
                throw new RuntimeException("Invalid database connection " + connectionBuilder);
            }
            return Populator.build(this);
        }

        /**
         * Tries to find the dataset directory starting from the current directory.
         */
        private void findDirectory() {
            for (File dir = new File("."); dir != null; dir = dir.getParentFile()) {
                File datasetsDirectory = new File(dir, "src/test/resources/datasets");
                if (datasetsDirectory.isDirectory()) {
                    try {
                        this.directory = datasetsDirectory.getAbsoluteFile().getCanonicalFile();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        /**
         * Loads the properties from ~/dbpop.properties
         */
        private void setupConnectionFromExternal() {
            String userHome = System.getProperty("user.home");
            if (userHome == null) throw new RuntimeException("Cannot find your home directory");
            File propertyFile = new File(userHome, "dbpop.properties");
            if (propertyFile.exists()) {
                Properties properties = new Properties();
                try (BufferedReader bufferedReader = Files.newBufferedReader(propertyFile.toPath(), StandardCharsets.UTF_8)) {
                    properties.load(bufferedReader);
                    String jdbcurl = properties.getProperty("jdbcurl");
                    String username = properties.getProperty("username");
                    String password = properties.getProperty("password");
                    if (jdbcurl == null) throw new RuntimeException("jdbcurl not set in " + propertyFile);
                    if (username == null) throw new RuntimeException("username not set in " + propertyFile);
                    if (password == null) throw new RuntimeException("password not set in " + propertyFile);

                    setConnectionBuilder(new UrlConnectionBuilder(jdbcurl, username, password));

                    setVerbose(Boolean.parseBoolean(properties.getProperty("verbose", "false")));
                    if (this.verbose) {
                        System.out.println("Properties loaded from " + propertyFile);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException("Could not find connection properties");
            }
        }
    }
}
