package org.dandoy.dbpopd.setup;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.utils.ExceptionUtils;
import org.dandoy.dbpop.utils.StringUtils;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.populate.PopulateService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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
                if (!checkSetupSql(targetConnection)) return;
            } finally {
                safeClose(targetConnection);
            }
            if (loadDatasets) {
                if (!checkPopulate()) return;
            }
            if (!checkSourceConnection()) return;
            setActivity(null);
        } catch (RuntimeException e) {
            setError(e.getMessage());
        }
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
            return true;
        } catch (Exception e) {
            setError(e);
            return false;
        }
    }

    private boolean checkSetupSql(Connection targetConnection) {
        setActivity("Executing the setup script");

        try {
            File configurationDir = configurationService.getConfigurationDir();
            File setupFile = new File(configurationDir, "setup.sql");
            if (setupFile.isFile()) {
                log.info("Loading " + setupFile);
                List<String> lines = Files.readAllLines(setupFile.toPath());
                List<Sql> sqls = linesToSql(lines);
                return executeSqls(setupFile, targetConnection, sqls);
            }
        } catch (Exception e) {
            setError(e);
            return false;
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
                    statement.execute(sql.sql);
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
