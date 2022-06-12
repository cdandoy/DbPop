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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Populator implements AutoCloseable {
    private final Database database;
    private final Map<String, Dataset> datasetsByName;
    private final Map<TableName, Table> tablesByName;
    private final boolean verbose;

    private Populator(Database database, Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, boolean verbose) {
        this.database = database;
        this.datasetsByName = datasetsByName;
        this.tablesByName = tablesByName;
        this.verbose = verbose;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Populator build() {
        return builder().build();
    }

    private static Populator build(Builder builder) {
        Database database = Database.createDatabase(builder.connection);
        List<Dataset> allDatasets = getDatasets(builder.directory);
        Set<String> allCatalogs = getCatalogs(allDatasets);
        Collection<Table> allTables = database.getTables(allCatalogs);
        Map<String, Dataset> datasetsByName = allDatasets.stream().collect(Collectors.toMap(Dataset::getName, Function.identity()));
        Map<TableName, Table> tablesByName = allTables.stream().collect(Collectors.toMap(Table::getTableName, Function.identity()));
        return new Populator(database, datasetsByName, tablesByName, builder.verbose);
    }

    @Override
    public void close() {
        database.close();
    }

    public int load(String... datasets) {
        return load(Arrays.asList(datasets));
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
                datasets.add(
                        new Dataset(
                                datasetFile.getName(),
                                dataFiles
                        )
                );
            }
        }
        return datasets;
    }

    private static Set<String> getCatalogs(List<Dataset> datasets) {
        return datasets.stream()
                .map(Dataset::getDataFiles)
                .flatMap(Collection::stream)
                .map(it -> it.getTableName().getCatalog())
                .collect(Collectors.toSet());
    }

    public static class Builder {
        private Connection connection;
        private File directory;
        private boolean verbose;

        public Builder() {
        }

        public Builder setConnection(Connection connection) {
            try {
                connection.getMetaData(); // Just to test the connection
                this.connection = connection;
                return this;
            } catch (SQLException e) {
                throw new RuntimeException("Failed to connect to the database", e);
            }
        }

        public Builder setConnection(String url, String username, String password) {
            try {
                return setConnection(DriverManager.getConnection(url, username, password));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

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

        public Builder setDirectory(String path) {
            return setDirectory(new File(path));
        }

        public Builder setVerbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        public Populator build() {
            if (directory == null) findDirectory();
            if (directory == null) throw new RuntimeException("You must specify a dataset directory");

            if (connection == null) setupConnectionFromExternal();
            if (connection == null) throw new RuntimeException("You must specify the database connection");

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

        private void setupConnectionFromExternal() {
            String userHome = System.getProperty("user.home");
            if (userHome == null) throw new RuntimeException("Cannot find your home directory");
            File propertyFile = new File(userHome, "dbpop.properties");
            if (propertyFile.exists()) {
                Properties properties = new Properties();
                try (BufferedReader bufferedReader = Files.newBufferedReader(propertyFile.toPath(), StandardCharsets.UTF_8)) {
                    properties.load(bufferedReader);
                    setupConnection(
                            propertyFile,
                            properties.getProperty("jdbcurl"),
                            properties.getProperty("username"),
                            properties.getProperty("password"),
                            properties.getProperty("verbose", "false")
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException("Could not find connection properties");
            }
        }

        private void setupConnection(File sourceFile, String jdbcurl, String username, String password, String verbose) {
            if (jdbcurl == null) throw new RuntimeException("jdbcurl not set in " + sourceFile);
            if (username == null) throw new RuntimeException("username not set in " + sourceFile);
            if (password == null) throw new RuntimeException("password not set in " + sourceFile);
            try {
                Connection connection = DriverManager.getConnection(jdbcurl, username, password);
                setConnection(connection);
                setVerbose(Boolean.parseBoolean(verbose));
            } catch (SQLException e) {
                throw new RuntimeException("Cannot connect to the database using " + sourceFile);
            }
        }
    }
}
