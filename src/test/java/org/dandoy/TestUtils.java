package org.dandoy;

import org.apache.commons.io.IOUtils;
import org.dandoy.dbpop.upload.Populator;
import org.dandoy.dbpop.utils.Env;
import org.dandoy.test.SqlServerTests;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Used by JUnit: <pre>@EnabledIf("org.dandoy.TestUtils#hasDatabaseSetup")</pre>
 */
@SuppressWarnings("unused")
public class TestUtils {
    private static final Env env = Objects.requireNonNull(Env.createEnv());

    public static boolean hasDatabaseSetup() {
        return env.getString("jdbcurl").startsWith("jdbc:");
    }

    public static boolean isSqlServer() {
        return env.environment(null).getString("jdbcurl").startsWith("jdbc:sqlserver:");
    }

    public static boolean hasSqlServer() {
        return env.environment("mssql").getString("jdbcurl").startsWith("jdbc:sqlserver:");
    }

    public static boolean isPostgres() {
        return env.environment(null).getString("jdbcurl").startsWith("jdbc:postgresql:");
    }

    public static boolean hasPostgres() {
        return env.environment("pgsql").getString("jdbcurl").startsWith("jdbc:postgresql:");
    }

    public static void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        if (!directoryToBeDeleted.delete()) {
            throw new RuntimeException("Failed to delete " + directoryToBeDeleted);
        }
    }

    public static void executeSqlScript(String environment, String path) {
        try (Connection connection = Populator.builder()
                .setEnvironment(environment)
                .getConnectionBuilder()
                .createConnection()) {
            try (InputStream resourceAsStream = SqlServerTests.class.getResourceAsStream(path)) {
                if (resourceAsStream == null) throw new RuntimeException("Script not found: " + path);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream, StandardCharsets.UTF_8))) {
                    String sql = IOUtils.toString(reader);
                    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                        preparedStatement.execute();
                    }
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
