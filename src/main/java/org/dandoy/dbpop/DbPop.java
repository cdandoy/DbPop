package org.dandoy.dbpop;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Command(name = "DbPop", version = "DbPop 0.1", mixinStandardHelpOptions = true)
public class DbPop implements Callable<Integer> {
    @Option(names = {"-j", "--jdbcurl"}, description = "Database URL")
    private String dbUrl = "jdbc:sqlserver://localhost;database=tempdb;trustServerCertificate=true";

    @Option(names = {"-u", "--username"}, description = "Database user")
    private String dbUser = "sa";

    @Option(names = {"-p", "--password"}, description = "Database password", required = true)
    private String dbPassword;

    @Option(names = {"-d", "--directory"}, description = "Dataset Directory")
    private File directory = new File(".");

    @Option(names = {"-v", "--verbose"}, description = "Verbose")
    private boolean verbose;

    @Parameters(paramLabel = "<dataset>", description = "Datasets", arity = "1..*")
    private final List<String> datasets = new ArrayList<>();

    public static void main(String[] args) {
        int exitCode = new CommandLine(new DbPop()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            long t0 = System.currentTimeMillis();
            int rowCount;
            directory = directory.getAbsoluteFile().getCanonicalFile();
            if (!directory.isDirectory()) throw new RuntimeException("Invalid dataset directory: " + directory);

            try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
                try (Database database = Database.createDatabase(connection).setVerbose(verbose)) {
                    List<Dataset> datasets = getDatasets(directory);
                    Set<String> catalogs = getCatalogs(datasets);
                    Collection<Table> tables = database.getTables(catalogs);
                    try (Populator populator = new Populator(database, datasets, tables).setVerbose(verbose)) {
                        rowCount = populator.load(this.datasets);
                    }
                }
            }
            long t1 = System.currentTimeMillis();
            System.out.printf("Loaded %d rows in %dms%n", rowCount, t1 - t0);
            return 0;
        } catch (Exception e) {
            if (verbose) {
                e.printStackTrace(System.err);
            } else {
                printCauses(e);
            }
            return 1;
        }
    }

    private void printCauses(Throwable t) {
        StringBuilder indent = new StringBuilder();
        while (t != null) {
            if (t.getMessage() != null) {
                System.err.println(indent + t.getMessage());
                indent.append("  ");
            }
            t = t.getCause();
        }
    }

    private List<Dataset> getDatasets(File directory) {
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

    private Set<String> getCatalogs(List<Dataset> datasets) {
        return datasets.stream()
                .map(Dataset::getDataFiles)
                .flatMap(Collection::stream)
                .map(it -> it.getTableName().getCatalog())
                .collect(Collectors.toSet());
    }
}
