package org.dandoy.test;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.LocalCredentials;
import org.dandoy.TestUtils;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.download.Downloader;
import org.dandoy.dbpop.upload.Populator;
import org.dandoy.dbpop.utils.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIf("org.dandoy.TestUtils#hasSqlServer")
@Slf4j
public class SqlServerTests {

    @BeforeAll
    public static void prepare() {
        TestUtils.executeSqlScript("mssql", "/mssql/test.sql");
    }

    @Test
    void noCsvFiles() {
        assertThrows(RuntimeException.class, () -> {
            try (Populator ignored = LocalCredentials
                    .mssqlPopulator()
                    .setDirectory("src/test/resources/test_no_datafiles/")
                    .build()) {
                System.out.println("I should not be here");
            }
        });
    }

    @Test
    void datasetNotFound() {
        assertThrows(RuntimeException.class, () -> {
            try (Populator populator = LocalCredentials
                    .mssqlPopulator()
                    .setDirectory("src/test/resources/tests/")
                    .build()) {
                populator.load("test_1_1");
            }
        });
    }

    @Test
    void tableDoesNotExist() {
        RuntimeException runtimeException = assertThrows(RuntimeException.class, () -> {
            try (Populator ignored = LocalCredentials
                    .mssqlPopulator()
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
        try (Populator populator = LocalCredentials
                .mssqlPopulator()
                .setDirectory("src/test/resources/test_expressions")
                .build()) {
            populator.load("base");
        }
    }

    @Test
    void testSingleton() {
        LocalCredentials
                .mssqlPopulator()
                .setDirectory("src/test/resources/test_expressions")
                .createSingletonInstance();

        Populator.getInstance().load("base");
    }

    @Test
    void mainTest() throws SQLException {
        try (Populator populator = LocalCredentials
                .mssqlPopulator()
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

    @Test
    void testStatic() throws SQLException {
        // product is in the static dataset and should only be loaded once per populator.
        try (Populator populator = LocalCredentials
                .mssqlPopulator()
                .setDirectory("src/test/resources/mssql")
                .build()) {
            try (Connection connection = populator.createConnection()) {

                populator.load("base");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 0);
                assertCount(connection, "invoice_details", 0);
                assertCount(connection, "products", 3);

                // Insert a product (static dataset) before to load. the populator is not supposed to notice
                try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO master.dbo.products (part_no, part_desc) VALUES ('XXX', 'XXX')")) {
                    preparedStatement.execute();
                }
                populator.load("base");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 0);
                assertCount(connection, "invoice_details", 0);
                assertCount(connection, "products", 4);
            }
        }

        // A new populator will reset products
        try (Populator populator = LocalCredentials
                .mssqlPopulator()
                .setDirectory("src/test/resources/mssql")
                .build()) {
            try (Connection connection = populator.createConnection()) {
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

    private static File createGeneratedTestDirectory() {
        try {
            URL resource = SqlServerTests.class.getClassLoader().getResource("logback.xml");
            URL generatedTestsUrl = new URL(resource, "generated_tests");
            File dir = Paths.get(generatedTestsUrl.toURI()).toFile();
            FileUtils.deleteRecursively(dir);
            assertTrue(dir.mkdirs() || dir.isDirectory());
            return dir;
        } catch (MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This test only works when running from the IDE because it is too complicated to change the classpath
     */
    @Test
    void testBinary() throws SQLException {
        File dir = createGeneratedTestDirectory();
        try (Downloader downloader = LocalCredentials
                .mssqlDownloader()
                .setDirectory(dir)
                .setDataset("base")
                .build()) {
            Connection connection = downloader.getConnection();

            try (Statement statement = connection.createStatement()) {
                statement.execute("DELETE FROM master.dbo.test_binary WHERE id = 1");
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
            connection.commit();
        }

        try (Populator populator =LocalCredentials
                .mssqlPopulator()
                .setDirectory(dir)
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
    }

    @Test
    void testLoadBinary() throws SQLException {
        try (Populator populator = LocalCredentials
                    .mssqlPopulator()
                .setDirectory("src/test/resources/mssql")
                .build()) {

            // Upload the data
            populator.load("test_binary");

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
    }

    @Test
    void testAppend() throws SQLException, IOException {

        // Load the default dataset
        try (Populator populator = LocalCredentials
                    .mssqlPopulator()
                .setDirectory("src/test/resources/mssql")
                .build()) {
            populator.load("base");
        }

        // Download it in a temp directory
        File dir = createGeneratedTestDirectory();
        try (Downloader downloader = LocalCredentials
                .mssqlDownloader()
                .setDirectory(dir)
                .setDataset("base")
                .build()) {
            downloader.download(new TableName("master", "dbo", "customers"));
        }

        // Insert a new customer
        try (Populator populator = LocalCredentials
                .mssqlPopulator()
                .setDirectory(dir)
                .build()) {
            try (Connection connection = populator.createConnection()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("INSERT INTO master.dbo.customers (name) VALUES ('HyperAir')");
                }
            }

            // then download in append mode
            try (Downloader downloader = LocalCredentials
                    .mssqlDownloader()
                    .setDirectory(dir)
                    .setDataset("base")
                    .build()) {
                // This is supposed to only download the new customer
                downloader.download(new TableName("master", "dbo", "customers"));
            }

            List<String> lines = Files.readAllLines(new File(dir, "base/master/dbo/customers.csv").toPath());
            assertEquals("customer_id,name", lines.get(0));                                     // Check that the first line is the header
            assertEquals(1, lines.stream().filter(it -> it.contains("customer_id")).count());   // Check that we only have one header
            assertEquals(1, lines.stream().filter(it -> it.contains("AirMethod")).count());     // Check that we still have the other customers
            assertTrue(lines.get(lines.size() - 1).contains("HyperAir"));                       // Check that the last added line is our inserted customer
        }
    }
}
