package org.dandoy.dbpopd.setup;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.utils.ExceptionUtils;
import org.dandoy.dbpop.utils.StringUtils;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.populate.PopulateService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
@Context
@Slf4j
public class SetupService {
    private final ConfigurationService configurationService;
    private final PopulateService populateService;
    // When running the tests, we don't want the data to be preloaded
    private final boolean loadDatasets;
    // When running the tests, we do not want the setup to run in a background thread
    private final boolean parallel;
    @Getter
    private SetupState setupState = new SetupState("Loading", null);

    @SuppressWarnings("MnInjectionPoints")
    public SetupService(
            ConfigurationService configurationService,
            PopulateService populateService,
            @Property(name = "dbpopd.startup.loadDatasets", defaultValue = "true") boolean loadDatasets,
            @Property(name = "dbpopd.startup.parallel", defaultValue = "true") boolean parallel
    ) {
        this.configurationService = configurationService;
        this.populateService = populateService;
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

    private void setActivity(String activity) {
        setupState = new SetupState(activity, null);
    }

    private boolean setError(String format, Object... params) {
        if (format == null) format = "Unknown Error";
        setupState = new SetupState(setupState.activity(), format.formatted(params));
        return false;
    }

    private void setError(Exception e) {
        setError(String.join("<br/>", ExceptionUtils.getErrorMessages(e)));
    }

    private void doit() {
        try {
            if (!checkDatasetDirectory()) return;
            if (!checkTargetSettings()) return;

            Connection targetConnection = checkTargetConnection();
            if (targetConnection == null) return;
            try {
                if (!executeInstall(targetConnection)) return;
                if (!executeStartup()) return;
            } finally {
                safeClose(targetConnection);
            }
            if (loadDatasets) {
                if (!checkPopulate()) return;
            }
            if (!checkSourceConnection()) return;
            setActivity(null);
        } catch (RuntimeException e) {
            log.error("Failed to setup", e);
            setError(e.getMessage());
        }
    }

    private boolean executeInstall(Connection targetConnection) {
        File setupDirectory = configurationService.getSetupDirectory();
        File installComplete = new File(setupDirectory, "install-complete.txt");
        if (!installComplete.exists()) {
            if (!executeScript(new File(setupDirectory, "pre-install.sh"))) {
                return false;
            }

            File[] sqlFiles = setupDirectory.listFiles((dir, name) -> name.endsWith(".sql"));
            if (sqlFiles != null) {
                ArrayList<File> sqlFileList = new ArrayList<>(List.of(sqlFiles));
                sqlFileList.sort(Comparator.comparing(File::getAbsolutePath));
                for (File sqlFile : sqlFileList) {
                    if (!executeSql(targetConnection, sqlFile)) {
                        return false;
                    }
                }
            }
            if (!executeScript(new File(setupDirectory, "post-install.sh"))) {
                return false;
            }
            try (BufferedWriter writer = Files.newBufferedWriter(installComplete.toPath())) {
                writer.write("install executed " + ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
            } catch (IOException e) {
                setError("Failed to write " + installComplete);
            }
        }
        return true;
    }

    private boolean executeSql(Connection connection, File file) {
        try {
            if (!file.exists()) return false;
            String absolutePath = file.getCanonicalPath();
            setActivity("Executing " + absolutePath);
            List<String> lines = Files.readAllLines(file.toPath());
            List<Sql> sqls = linesToSql(lines);
            return executeSqls(file, connection, sqls);
        } catch (IOException | SQLException e) {
            setError(e);
            return false;
        }
    }

    private boolean executeScript(File file) {
        if (!file.exists()) return false;
        try {
            String absolutePath = file.getCanonicalPath();
            setActivity("Executing " + absolutePath);

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
                setError("%s returned %d", absolutePath, exitValue);
                return false;
            }
            return true;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean executeStartup() {
        setActivity("Executing startup.sh");
        File setupDirectory = configurationService.getSetupDirectory();
        return executeScript(new File(setupDirectory, "startup.sh"));
    }

    private boolean checkDatasetDirectory() {
        setActivity("Verifying the configuration");
        File configurationDir = configurationService.getConfigurationDir();
        String[] paths = {".", "datasets", "datasets/static", "datasets/base"};
        for (String path : paths) {
            File dir = new File(configurationDir, path);
            if (!dir.mkdirs() && !dir.isDirectory()) {
                return setError("Invalid directory %s", dir);
            }
        }

        return true;
    }

    private boolean checkPopulate() {
        setActivity("Populating static and base datasets");
        try {
            populateService.populate(List.of(Datasets.STATIC, Datasets.BASE));
        } catch (Exception e) {
            // The dataset has already been marked as failed
            log.error("Failed to load static+base", e);
        }
        return true;
    }

    private boolean checkTargetSettings() {
        setActivity("Connecting to the target database");
        if (!configurationService.hasTargetConnection()) {
            setError("Target database not configured");
            return false;
        }
        return true;
    }

    private Connection checkTargetConnection() {
        setActivity("Connecting to the target database");
        try {
            return configurationService.createTargetConnection();
        } catch (SQLException e) {
            setError(e.getMessage());
        }
        return null;
    }

    private boolean checkSourceConnection() {
        setActivity("Connecting to the source database");
        if (configurationService.hasSourceConnection()) {
            //noinspection EmptyTryBlock
            try (Connection ignored = configurationService.getSourceConnectionBuilder().createConnection()) {
            } catch (SQLException e) {
                setError(e.getMessage());
                return false;
            }
        }
        return true;
    }

    private void safeClose(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            log.error("Error closing the connection", e);
        }
    }

    static List<Sql> linesToSql(List<String> lines) {
        Pattern go = Pattern.compile("^ *go[ ;]*$", Pattern.CASE_INSENSITIVE);
        Pattern sc = Pattern.compile("^(.*) *; *$");

        SqlAccumulator accumulator = new SqlAccumulator();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);

            Matcher scMatcher = sc.matcher(line);
            if (scMatcher.matches()) {
                String sqlMatch = scMatcher.group(1);
                if (!sqlMatch.isEmpty() && !go.matcher(sqlMatch).matches()) {
                    accumulator.addLine(sqlMatch);
                }
                accumulator.finishSql(i);
            } else if (go.matcher(line).matches()) {
                accumulator.finishSql(i);
            } else if (line.trim().isEmpty()) {
                accumulator.finishSql(i);
            } else {
                accumulator.addLine(line);
            }
        }

        accumulator.finishSql(0);
        return accumulator.getSqls();
    }

    private boolean executeSqls(File setupFile, Connection connection, List<Sql> sqls) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (Sql sql : sqls) {
                try {
                    boolean isSelect = statement.execute(sql.sql);
                    for (SQLWarning warnings = statement.getWarnings();
                         warnings != null;
                         warnings = warnings.getNextWarning()) {
                        String message = warnings.getMessage();
                        System.err.println(message);
                    }
                    if (isSelect) {
                        try (ResultSet resultSet = statement.getResultSet()) {
                            ResultSetMetaData metaData = resultSet.getMetaData();

                            // Print the headers
                            StringBuilder sb = new StringBuilder();
                            for (int i = 0; i < metaData.getColumnCount(); i++) {
                                if (i > 0) sb.append('\t');
                                sb.append(metaData.getColumnLabel(i + 1));
                            }
                            if (!sb.isEmpty()) {
                                System.out.println(sb);
                                System.out.println("_".repeat(sb.length()));
                            }

                            // Print the values
                            while (resultSet.next()) {
                                sb.setLength(0);
                                for (int i = 0; i < metaData.getColumnCount(); i++) {
                                    if (i > 0) sb.append('\t');
                                    sb.append(resultSet.getString(i + 1));
                                }
                                System.out.println(sb);
                            }
                        }
                    }
                } catch (SQLException e) {
                    String filename = setupFile.getAbsolutePath();
                    try {
                        filename = setupFile.getCanonicalPath();
                    } catch (IOException ignored) {
                    }
                    String errorMessage = """
                            Error at %s:%d
                            %s
                            ==> %s
                            """
                            .formatted(
                                    filename,
                                    sql.line,
                                    sql.sql,
                                    e.getMessage()
                            );
                    setError(errorMessage);
                    return false;
                }
            }
        }
        return true;
    }

    private static class SqlAccumulator {
        private int lineNo = 0;
        @Getter
        private final List<Sql> sqls = new ArrayList<>();
        private final StringBuilder stringBuilder = new StringBuilder();

        void addLine(String line) {
            stringBuilder.append(line).append("\n");
        }

        void finishSql(int lineNo) {
            String sqlString = stringBuilder.toString();
            sqlString = StringUtils.removeEnd(sqlString, "\n");
            if (!sqlString.isEmpty()) {
                sqls.add(new Sql(this.lineNo + 1, sqlString)); // +1 because this.lineNo is 0-based
            }
            stringBuilder.setLength(0);
            this.lineNo = lineNo + 1; // +1 because the next statement starts on the next line
        }
    }

    record Sql(int line, String sql) {
    }

    public record SetupState(String activity, String error) {}
}
