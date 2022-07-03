package org.dandoy.test;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.download.Downloader;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIf("org.dandoy.TestEnv#hasDatabaseSetup")
@Slf4j
public class TestBinary {
    @Test
    void mainTest() throws SQLException, IOException {
        File tempDirectory = Files.createTempDirectory("DbPopTestBinary").toFile();
        try {
            try (Downloader downloader = Downloader.builder()
                    .setDirectory(tempDirectory)
                    .setDataset("base")
                    .build()) {
                Connection connection = downloader.getConnection();

                // Creates and populates the test table
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE IF EXISTS master.dbo.test_binary");
                    statement.execute("CREATE TABLE master.dbo.test_binary(id INT PRIMARY KEY, test_binary BINARY(32), test_blob VARBINARY(MAX))");
                }
                try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO master.dbo.test_binary(id, test_binary, test_blob) VALUES (?,?,?)")) {
                    preparedStatement.setInt(1, 1);
                    preparedStatement.setBytes(2, "HELLOHELLOHELLOHELLOHELLOHELLOHE".getBytes());
                    Blob blob = connection.createBlob();
                    blob.setBytes(1, "HELLO\0WORLD".getBytes());
                    preparedStatement.setBlob(3, blob);
                    preparedStatement.executeUpdate();
                }

                // Download the data
                downloader.download("master.dbo.test_binary");

                // Don't leave that data behind
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DELETE FROM master.dbo.test_binary WHERE id = 1");
                }
            }

            try (Populator populator = Populator.builder()
                    .setDirectory(tempDirectory)
                    .build()) {

                // Upload the data
                populator.load("base");

                // Verify
                try (Connection connection = populator.createConnection()) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM master.dbo.test_binary")) {
                        try (ResultSet resultSet = preparedStatement.executeQuery()) {
                            assertTrue(resultSet.next());
                            assertEquals(1, resultSet.getInt(1));
                            assertEquals("HELLOHELLOHELLOHELLOHELLOHELLOHE", new String(resultSet.getBytes(2)));
                            Blob blob = resultSet.getBlob(3);
                            assertEquals("HELLO\0WORLD", new String(blob.getBytes(1, (int) blob.length())));
                        }
                    }
                }
            }
        } finally {
            if (!deleteDirectory(tempDirectory)) {
                log.error("Failed to delete {}", tempDirectory);
            }
        }
    }

    private static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
