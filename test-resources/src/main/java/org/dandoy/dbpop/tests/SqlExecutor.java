package org.dandoy.dbpop.tests;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SqlExecutor {
    static List<String> splitMsSqlScript(String script) {
        List<String> ret = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        String[] lines = StringUtils.split(script.replace("\r", ""), "\n");
        for (String line : lines) {
            if (line.trim().equalsIgnoreCase("go")) {
                ret.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(line).append("\n");
            }
        }
        if (sb.length() > 0) {
            ret.add(sb.toString());
        }
        return ret;
    }

    private static List<String> splitScript(Connection sourceConnection, String script) {
        if (sourceConnection instanceof ISQLServerConnection) {
            return splitMsSqlScript(script);
        } else {
            return List.of(script);
        }
    }

    public static void execute(Connection sourceConnection, String... filenames) {
        for (String filename : filenames) {
            InputStream resourceAsStream = SqlExecutor.class.getResourceAsStream(filename);
            if (resourceAsStream == null) throw new RuntimeException("File not found: " + filename);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream, StandardCharsets.UTF_8))) {
                String fullScript = IOUtils.toString(reader);
                for (String sql : splitScript(sourceConnection, fullScript)) {
                    try (PreparedStatement preparedStatement = sourceConnection.prepareStatement(sql)) {
                        preparedStatement.execute();
                    } catch (SQLException e) {
                        throw new RuntimeException("Failed to execute \n%s".formatted(sql), e);
                    }
                }
                sourceConnection.commit();
            } catch (Exception e) {
                throw new RuntimeException("Failed to load " + filename, e);
            }
        }
    }
}
