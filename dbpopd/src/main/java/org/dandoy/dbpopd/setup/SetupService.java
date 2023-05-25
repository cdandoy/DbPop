package org.dandoy.dbpopd.setup;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.datasets.DatasetsService;
import org.dandoy.dbpopd.populate.PopulateService;
import org.dandoy.dbpopd.status.StatusService;
import org.dandoy.dbpopd.utils.SqlExecuteUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Singleton
@Context
@Slf4j
public class SetupService {
    private final ConfigurationService configurationService;
    private final PopulateService populateService;
    private final DatasetsService datasetsService;
    private final StatusService statusService;
    // When running the tests, we don't want the data to be preloaded
    private final boolean loadDatasets;
    // When running the tests, we do not want the setup to run in a background thread
    private final boolean parallel;

    @SuppressWarnings("MnInjectionPoints")
    public SetupService(
            ConfigurationService configurationService,
            PopulateService populateService,
            DatasetsService datasetsService,
            StatusService statusService,
            @Property(name = "dbpopd.startup.loadDatasets", defaultValue = "true") boolean loadDatasets,
            @Property(name = "dbpopd.startup.parallel", defaultValue = "true") boolean parallel
    ) {
        this.configurationService = configurationService;
        this.populateService = populateService;
        this.datasetsService = datasetsService;
        this.statusService = statusService;
        this.loadDatasets = loadDatasets;
        this.parallel = parallel;
    }

    @PostConstruct
    public void loadSetup() {
        if (parallel) {
            new Thread(this::doit).start();
        } else {
            doit();
        }
    }

    private void doit() {
        try {
            checkDatasetDirectory();
            Connection targetConnection = checkTargetConnection();
            if (targetConnection != null) {
                executeInstall(targetConnection);

                executeStartup();
                if (loadDatasets) {
                    try {
                        checkPopulate();
                    } catch (Exception e) {
                        // Do not fail the startup because of a dataset error
                        log.error("Failed to load the static and base datasets", e);
                    }
                }
            }

            checkSourceConnection();

            statusService.setComplete(true);
        } catch (Exception e) {
            log.error("Failed to setup", e);
        }
    }

    private void executeInstall(Connection targetConnection) {
        File setupDirectory = configurationService.getSetupDirectory();
        if (setupDirectory.exists()) {
            File installComplete = new File(setupDirectory, "install-complete.txt");
            if (!installComplete.exists()) {
                executeScript(new File(setupDirectory, "pre-install.sh"));

                File[] sqlFiles = setupDirectory.listFiles((dir, name) -> name.endsWith(".sql"));
                if (sqlFiles != null) {
                    ArrayList<File> sqlFileList = new ArrayList<>(List.of(sqlFiles));
                    sqlFileList.sort(Comparator.comparing(File::getAbsolutePath));
                    for (File sqlFile : sqlFileList) {
                        executeSql(targetConnection, sqlFile);
                    }
                }
                executeScript(new File(setupDirectory, "post-install.sh"));

                statusService.run("install-complete.txt", () -> {
                    try (BufferedWriter writer = Files.newBufferedWriter(installComplete.toPath())) {
                        writer.write("install executed " + ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
                    }
                });
            }
        }
    }

    private void executeSql(Connection connection, File file) {
        if (file.exists()) {
            String canonicalPath;
            try {
                canonicalPath = file.getCanonicalPath();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            statusService.run(canonicalPath, () -> SqlExecuteUtils.executeSqlFile(connection, file));
        }
    }

    private void executeScript(File file) {
        if (file.exists()) {
            statusService.run(file.getName(), () -> {
                String absolutePath = file.getCanonicalPath();

                List<String> args;
                if (File.separatorChar == '/') {
                    args = List.of("/bin/sh", "-c", absolutePath);
                } else {
                    // Not great but this is for dev. mode on Windows only
                    args = List.of("bash", file.getName());
                }

                ProcessBuilder processBuilder = new ProcessBuilder(args);
                // I am not comfortable passing those environment variables to a shell script especially if they point dbpop at a prod DB.
                Map<String, String> environment = processBuilder.environment();
                environment.remove("SOURCE_JDBCURL");
                environment.remove("SOURCE_USERNAME");
                environment.remove("SOURCE_PASSWORD");
                ConnectionBuilder targetConnectionBuilder = configurationService.getTargetConnectionBuilder();
                if (targetConnectionBuilder instanceof UrlConnectionBuilder urlConnectionBuilder) {
                    environment.put("TARGET_JDBCURL", urlConnectionBuilder.getUrl());
                    environment.put("TARGET_USERNAME", urlConnectionBuilder.getUsername());
                    environment.put("TARGET_PASSWORD", urlConnectionBuilder.getPassword());
                }

                int exitValue = processBuilder
                        .directory(file.getParentFile())
                        .inheritIO()
                        .start()
                        .waitFor();
                if (exitValue != 0) {
                    throw new RuntimeException("%s returned %d".formatted(absolutePath, exitValue));
                }
            });
        }
    }

    private void executeStartup() {
        statusService.run("startup.sh", () -> {
            File setupDirectory = configurationService.getSetupDirectory();
            executeScript(new File(setupDirectory, "startup.sh"));
        });
    }

    private void checkDatasetDirectory() {
        statusService.run("Verifying the configuration", () -> {
            File configurationDir = configurationService.getConfigurationDir();
            String[] paths = {".", "datasets", "datasets/static", "datasets/base"};
            for (String path : paths) {
                File dir = new File(configurationDir, path);
                if (!dir.mkdirs() && !dir.isDirectory()) {
                    throw new RuntimeException("Invalid directory " + dir);
                }
            }
        });
    }

    private void checkPopulate() {
        statusService.run("Populating static and base datasets", () -> {
            if (datasetsService.canPopulate(Datasets.BASE)) {
                populateService.populate(List.of(Datasets.STATIC, Datasets.BASE));
            }
        });
    }

    private Connection checkTargetConnection() {
        if (configurationService.hasTargetConnection()) {
            return statusService.run("Connecting to the target database", () -> {
                ConnectionBuilder targetConnectionBuilder = configurationService.getTargetConnectionBuilder();
                targetConnectionBuilder.testConnection();
                return targetConnectionBuilder.createConnection();
            });
        } else {
            return null;
        }
    }

    private void checkSourceConnection() {
        if (configurationService.hasSourceConnection()) {
            statusService.run("Connecting to the source database", () -> {
                ConnectionBuilder sourceConnectionBuilder = configurationService.getSourceConnectionBuilder();
                sourceConnectionBuilder.testConnection();
                Connection connection = sourceConnectionBuilder.createConnection();
                connection.close();
            });
        }
    }
}
