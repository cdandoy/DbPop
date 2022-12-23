package org.dandoy.dbpopd;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.utils.StringUtils;

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
@Requires(property = "dbpopd.mode", value = "populate")
public class SqlSetupService {
    private final ConfigurationService configurationService;
    @Getter
    private boolean loading;
    @Getter
    private boolean loaded;
    @Getter
    private String errorMessage;

    public SqlSetupService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @PostConstruct
    public void loadSetup() {
        loading = true;
        try {
            File configurationDir = configurationService.getConfigurationDir();
            File setupFile = new File(configurationDir, "setup.sql");
            if (setupFile.isFile()) {
                try (Connection connection = configurationService.createConnection()) {
                    List<String> lines = Files.readAllLines(setupFile.toPath());
                    List<Sql> sqls = linesToSql(lines);
                    executeSqls(setupFile, connection, sqls);
                } catch (SQLException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (RuntimeException e) {
            errorMessage = e.getMessage();
        } finally {
            loading = false;
            loaded = true;
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

    private void executeSqls(File setupFile, Connection connection, List<Sql> sqls) throws SQLException {
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
                    errorMessage = """
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
                }
            }
        }
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
}
