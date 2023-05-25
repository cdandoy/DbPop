package org.dandoy.dbpopd.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.utils.ExceptionUtils;
import org.dandoy.dbpop.utils.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
public class SqlExecuteUtils {
    public static void executeSqlFile(Connection connection, File file) {
        try {
            List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.US_ASCII);
            List<Sql> sqls = linesToSql(lines);
            executeSqls(file, connection, sqls);
        } catch (MalformedInputException e) {
            throw new RuntimeException("Invalid encoding: %s".formatted(file));
        } catch (IOException | SQLException e) {
            log.error(file.getAbsolutePath(), e);
            String join = String.join("\n", ExceptionUtils.getErrorMessages(e));
            throw new RuntimeException("Failed to execute " + file + "\n" + join);
        }

    }

    public static List<Sql> linesToSql(List<String> lines) {
        Pattern go = Pattern.compile("^ *go[ ;]*$", Pattern.CASE_INSENSITIVE);

        SqlAccumulator accumulator = new SqlAccumulator();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);

            if (go.matcher(line).matches()) {
                accumulator.finishSql(i);
            } else {
                accumulator.addLine(line);
            }
        }

        accumulator.finishSql(0);
        return accumulator.getSqls();
    }

    public static void executeSqls(File setupFile, Connection connection, List<Sql> sqls) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (Sql sql : sqls) {
                try {
                    boolean isSelect = statement.execute(sql.sql());
                    for (SQLWarning warnings = statement.getWarnings();
                         warnings != null;
                         warnings = warnings.getNextWarning()) {
                        String message = warnings.getMessage();
                        System.out.println("\n" + message);
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
                    System.out.println();
                    log.error("""
                                    Error at {}:{}
                                    {}
                                    ==> {}
                                    """,
                            filename,
                            sql.line(),
                            sql.sql(),
                            e.getMessage()
                    );
                    log.error("Failed to execute {}", sql.sql());
                    log.error("", e);
//                  Do not fail if one statement fails. We may be importing sprocs that are in an invalid state
                }
            }
            System.out.println();
        }
    }

    public static class SqlAccumulator {
        private int lineNo = 0;
        @Getter
        private final List<Sql> sqls = new ArrayList<>();
        private final StringBuilder stringBuilder = new StringBuilder();

        void addLine(String line) {
            if (stringBuilder.length() == 0 && line.isEmpty()) return;
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

    public record Sql(int line, String sql) {}
}
