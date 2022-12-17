package org.dandoy;

import org.apache.commons.io.IOUtils;
import org.dandoy.test.SqlServerTests;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TestUtils {
    public static boolean hasSqlServer() {
        return LocalCredentials.from("mssql").dbUrl() != null;
    }

    public static boolean hasPostgres() {
        return LocalCredentials.from("pgsql").dbUrl() != null;
    }

    public static void executeSqlScript(String environment, String path) {
        try (Connection connection = LocalCredentials
                .from(environment)
                .populator()
                .getConnectionBuilder()
                .createConnection()) {
            try (InputStream resourceAsStream = SqlServerTests.class.getResourceAsStream(path)) {
                if (resourceAsStream == null) throw new RuntimeException("Script not found: " + path);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream, StandardCharsets.UTF_8))) {
                    String sql = IOUtils.toString(reader);
                    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                        preparedStatement.execute();
                    } catch (SQLException e) {
                        throw new RuntimeException("Failed to execute \n%s".formatted(sql), e);
                    }
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
