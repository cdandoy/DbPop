package org.dandoy.test;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.LocalCredentials;
import org.dandoy.TestUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.download.TableDownloader;
import org.dandoy.dbpop.upload.Populator;
import org.dandoy.dbpop.utils.FileUtils;
import org.junit.jupiter.api.BeforeEach;
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

import static org.dandoy.TestUtils.customers;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIf("org.dandoy.TestUtils#hasMssql")
@Slf4j
public class SqlServerTests {
    @BeforeEach
    void setUp() {
        TestUtils.prepareMssqlTarget();
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
            try (Connection targetConnection = populator.createTargetConnection()) {

                populator.load("base");
                assertCount(targetConnection, "customers", 3);
                assertCount(targetConnection, "invoices", 0);
                assertCount(targetConnection, "invoice_details", 0);
                assertCount(targetConnection, "products", 3);

                populator.load("base", "invoices");
                assertCount(targetConnection, "customers", 3);
                assertCount(targetConnection, "invoices", 4);
                assertCount(targetConnection, "invoice_details", 7);
                assertCount(targetConnection, "products", 3);

                populator.load("base");
                assertCount(targetConnection, "customers", 3);
                assertCount(targetConnection, "invoices", 0);
                assertCount(targetConnection, "invoice_details", 0);
                assertCount(targetConnection, "products", 3);
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
            try (Connection targetConnection = populator.createTargetConnection()) {

                populator.load("base");
                assertCount(targetConnection, "customers", 3);
                assertCount(targetConnection, "invoices", 0);
                assertCount(targetConnection, "invoice_details", 0);
                assertCount(targetConnection, "products", 3);

                // Insert a product (static dataset) before to load. the populator is not supposed to notice
                try (PreparedStatement preparedStatement = targetConnection.prepareStatement("INSERT INTO master.dbo.products (part_no, part_desc) VALUES ('XXX', 'XXX')")) {
                    preparedStatement.execute();
                }
                populator.load("base");
                assertCount(targetConnection, "customers", 3);
                assertCount(targetConnection, "invoices", 0);
                assertCount(targetConnection, "invoice_details", 0);
                assertCount(targetConnection, "products", 4);
            }
        }

        // A new populator will reset products
        try (Populator populator = LocalCredentials
                .mssqlPopulator()
                .setDirectory("src/test/resources/mssql")
                .build()) {
            try (Connection targetConnection = populator.createTargetConnection()) {
                populator.load("base");
                assertCount(targetConnection, "customers", 3);
                assertCount(targetConnection, "invoices", 0);
                assertCount(targetConnection, "invoice_details", 0);
                assertCount(targetConnection, "products", 3);
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

    @Test
    void testBinary() throws SQLException {
        TestUtils.prepareMssqlSource();
        File dir = createGeneratedTestDirectory();
        TestUtils.delete(dir);
        try (Connection sourceConnection = LocalCredentials.from("mssql").createSourceConnection()) {
            try (Database sourceDatabase = Database.createDatabase(sourceConnection)) {
                try (TableDownloader tableDownloader = TableDownloader.builder()
                        .setDatabase(sourceDatabase)
                        .setDatasetsDirectory(dir)
                        .setDataset("base")
                        .setTableName(new TableName("master", "dbo", "test_binary"))
                        .build()) {

                    // Download the data
                    tableDownloader.download();
                }
            }
        }

        try (Populator populator = LocalCredentials
                .mssqlPopulator()
                .setDirectory(dir)
                .build()) {

            // Upload the data
            populator.load("base");

            // Verify
            try (Connection targetConnection = populator.createTargetConnection()) {
                try (PreparedStatement preparedStatement = targetConnection.prepareStatement("SELECT * FROM master.dbo.test_binary")) {
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        assertTrue(resultSet.next());
                        assertEquals(1, resultSet.getInt(1));
                        assertEquals("HELLO\0WORLD", new String(resultSet.getBytes(2)));
                        Blob blob = resultSet.getBlob(3);
                        assertEquals("HELLO\0WORLD", new String(blob.getBytes(1, (int) blob.length())));
                    }
                }
            }
        }
    }

    @Test
    void testAppend() throws SQLException, IOException {
        TestUtils.prepareMssqlSource();
        File dir = createGeneratedTestDirectory();
        File customerCsv = new File(dir, "base/master/dbo/customers.csv");
        TestUtils.delete(dir);

        try (Connection sourceConnection = LocalCredentials.from("mssql").createSourceConnection()) {
            try (Database sourceDatabase = Database.createDatabase(sourceConnection)) {
                // Download it in a temp directory
                try (TableDownloader tableDownloader = TableDownloader.builder()
                        .setDatabase(sourceDatabase)
                        .setDatasetsDirectory(dir)
                        .setDataset("base")
                        .setTableName(customers)
                        .build()) {
                    tableDownloader.download();
                }

                assertEquals(4, Files.readAllLines(customerCsv.toPath()).size()); // Only the header

                // Insert a new customer
                try (Statement statement = sourceConnection.createStatement()) {
                    statement.execute("INSERT INTO master.dbo.customers (name) VALUES ('HyperAir')");
                }

                // then download in append mode
                try (TableDownloader tableDownloader = TableDownloader.builder()
                        .setDatabase(sourceDatabase)
                        .setDatasetsDirectory(dir)
                        .setDataset("base")
                        .setTableName(customers)
                        .build()) {
                    // This is supposed to only download the new customer
                    tableDownloader.download();
                }
                List<String> lines = Files.readAllLines(customerCsv.toPath());
                assertEquals("customer_id,name", lines.get(0));                                     // Check that the first line is the header
                assertEquals(1, lines.stream().filter(it -> it.contains("customer_id")).count());   // Check that we only have one header
                assertEquals(1, lines.stream().filter(it -> it.contains("AirMethod")).count());     // Check that we still have the other customers
                assertTrue(lines.get(lines.size() - 1).contains("HyperAir"));                       // Check that the last added line is our inserted customer
            }
        }
    }
}
