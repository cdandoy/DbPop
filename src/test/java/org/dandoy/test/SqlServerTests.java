package org.dandoy.test;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.TestUtils;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.download.Downloader;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIf("org.dandoy.TestUtils#hasSqlServer")
@Slf4j
public class SqlServerTests {
    @Test
    void noCsvFiles() {
        assertThrows(RuntimeException.class, () -> {
            try (Populator ignored = Populator.builder()
                    .setEnvironment("mssql")
                    .setDirectory("src/test/resources/test_no_datafiles")
                    .build()) {
                System.out.println("I should not be here");
            }
        });
    }

    @Test
    void datasetNotFound() {
        assertThrows(RuntimeException.class, () -> {
            try (Populator populator = Populator.builder()
                    .setEnvironment("mssql")
                    .setDirectory("src/test/resources/tests")
                    .build()) {
                populator.load("test_1_1");
            }
        });
    }

    @Test
    void tableDoesNotExist() {
        RuntimeException runtimeException = assertThrows(RuntimeException.class, () -> {
            try (Populator ignored = Populator.builder()
                    .setEnvironment("mssql")
                    .setDirectory("src/test/resources/test_bad_table")
                    .build()) {
                System.out.println("I should not be here");
            }
        });
        assertTrue(runtimeException.getMessage().contains("master.dbo.bad_table"));
        assertTrue(runtimeException.getMessage().contains("bad_table.csv"));
    }

    @Test
    void testExpressions() {
        try (Populator populator = Populator.builder()
                .setEnvironment("mssql")
                .setDirectory("src/test/resources/test_expressions")
                .build()) {
            populator.load("base");
        }
    }

    @Test
    void mainTest() throws SQLException {
        try (Populator populator = Populator.builder()
                .setEnvironment("mssql")
                .setDirectory("src/test/resources/mssql")
                .build()) {
            try (Connection connection = populator.createConnection()) {

                populator.load("base");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 0);
                assertCount(connection, "invoice_details", 0);
                assertCount(connection, "products", 3);

                populator.load("base", "invoices");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 4);
                assertCount(connection, "invoice_details", 7);
                assertCount(connection, "products", 3);

                populator.load("base");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 0);
                assertCount(connection, "invoice_details", 0);
                assertCount(connection, "products", 3);
            }
        }
    }

    public static void assertCount(Connection connection, String table, int expected) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM master.dbo." + table)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) throw new RuntimeException();
                int actual = resultSet.getInt(1);
                assertEquals(expected, actual, String.format("Invalid row count for table %s", table));
            }
        }
    }

    @Test
    void testBinary() throws SQLException, IOException {
        File tempDirectory = Files.createTempDirectory("DbPopTestBinary").toFile();
        try {
            try (Downloader downloader = Downloader.builder()
                    .setEnvironment("mssql")
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
                downloader.download(new TableName("master", "dbo", "test_binary"));

                // Don't leave that data behind
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DELETE FROM master.dbo.test_binary WHERE id = 1");
                }
            }

            try (Populator populator = Populator.builder()
                    .setEnvironment("mssql")
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
            if (!TestUtils.deleteDirectory(tempDirectory)) {
                log.error("Failed to delete {}", tempDirectory);
            }
        }
    }
}
