package org.dandoy.dbpopd.setup;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.event.ApplicationEventListener;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.ConnectionBuilderChangedEvent;
import org.dandoy.dbpopd.config.ConnectionType;
import org.dandoy.dbpopd.status.StatusService;
import org.dandoy.dbpopd.utils.SqlExecuteUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
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
public class SetupService implements ApplicationEventListener<ConnectionBuilderChangedEvent> {
    private final ConfigurationService configurationService;
    private final StatusService statusService;

    private boolean hasExecutedStartup;

    public SetupService(
            ConfigurationService configurationService,
            StatusService statusService
    ) {
        this.configurationService = configurationService;
        this.statusService = statusService;
    }

    @Override
    public void onApplicationEvent(ConnectionBuilderChangedEvent event) {
        if (event.type() == ConnectionType.TARGET) {
            ConnectionBuilder targetConnectionBuilder = event.connectionBuilder();
            // We only execute the startup scripts if we have received a target connection event
            if (targetConnectionBuilder != null && !hasExecutedStartup) {
                try (Connection targetConnection = targetConnectionBuilder.createConnection()) {
                    // Installation scripts are run only once
                    executeInstall(targetConnection);

                    // Startup scripts are run every time we start
                    executeStartup();

                    hasExecutedStartup = true;
                } catch (SQLException e) {
                    log.error("Failed to create the connection", e);
                }
            }
        }
    }

    @PostConstruct
    public void init() {
        try {
            checkDatasetDirectory();
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

                try (BufferedWriter writer = Files.newBufferedWriter(installComplete.toPath())) {
                    writer.write("install executed " + ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write to " + installComplete, e);
                }
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
            try {
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
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Failed to execute " + file, e);
            }
        }
    }

    private void executeStartup() {
        statusService.run("startup.sh", () -> {
            File setupDirectory = configurationService.getSetupDirectory();
            executeScript(new File(setupDirectory, "startup.sh"));
        });
    }

    private void checkDatasetDirectory() {
        File configurationDir = configurationService.getConfigurationDir();
        String[] paths = {".", "datasets", "datasets/static", "datasets/base"};
        for (String path : paths) {
            File dir = new File(configurationDir, path);
            if (!dir.mkdirs() && !dir.isDirectory()) {
                log.error("Failed to create the directory " + dir);
            }
        }
    }
}
