package org.dandoy.dbpop.tests;

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SqlExecutor {
    public static void execute(Connection sourceConnection, String... filenames) {
        for (String filename : filenames) {
            InputStream resourceAsStream = SqlExecutor.class.getResourceAsStream(filename);
            if (resourceAsStream == null) throw new RuntimeException("File not found: " + filename);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream, StandardCharsets.UTF_8))) {
                String sql = IOUtils.toString(reader);
                try (PreparedStatement preparedStatement = sourceConnection.prepareStatement(sql)) {
                    preparedStatement.execute();
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to execute \n%s".formatted(sql), e);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load " + filename, e);
            }
        }
    }
}
