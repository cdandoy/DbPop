package org.dandoy.test;

import org.dandoy.DbPopUtils;
import org.dandoy.LocalCredentials;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.download.TableDownloader;
import org.dandoy.dbpop.tests.TestUtils;
import org.dandoy.dbpop.upload.Populator;
import org.dandoy.dbpop.utils.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;

import static org.dandoy.DbPopUtils.customers;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlServerBinaryTest {
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
        DbPopUtils.prepareMssqlSource();
        File dir = createGeneratedTestDirectory();
        TestUtils.delete(dir);
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Database sourceDatabase = Database.createDatabase(localCredentials.sourceConnectionBuilder())) {
            try (TableDownloader tableDownloader = TableDownloader.builder()
                    .setDatabase(sourceDatabase)
                    .setDatasetsDirectory(dir)
                    .setDataset("base")
                    .setTableName(new TableName("dbpop", "dbo", "test_binary"))
                    .build()) {

                // Download the data
                tableDownloader.download();
            }
        }

        try (Database targetDatabase = Database.createDatabase(LocalCredentials.from("mssql").targetConnectionBuilder())) {
            Populator populator = Populator.createPopulator(targetDatabase, dir);

            // Upload the data
            populator.load("base");
        }

        try (Connection targetConnection = localCredentials.createTargetConnection()) {
            // Verify
            try (PreparedStatement preparedStatement = targetConnection.prepareStatement("SELECT * FROM dbpop.dbo.test_binary")) {
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

    @Test
    void testAppend() throws SQLException, IOException {
        DbPopUtils.prepareMssqlSource();
        File dir = createGeneratedTestDirectory();
        File customerCsv = new File(dir, "base/dbpop/dbo/customers.csv");
        TestUtils.delete(dir);

        ConnectionBuilder sourceConnectionBuilder = LocalCredentials.from("mssql").sourceConnectionBuilder();
        try (Database sourceDatabase = Database.createDatabase(sourceConnectionBuilder)) {
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
            try (Connection sourceConnection = sourceConnectionBuilder.createConnection()) {
                try (Statement statement = sourceConnection.createStatement()) {
                    statement.execute("INSERT INTO dbpop.dbo.customers (name) VALUES ('HyperAir')");
                }
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
            assertEquals("customer_id,customer_type_id,name", lines.get(0));                                     // Check that the first line is the header
            assertEquals(1, lines.stream().filter(it -> it.contains("customer_id")).count());   // Check that we only have one header
            assertEquals(1, lines.stream().filter(it -> it.contains("AirMethod")).count());     // Check that we still have the other customers
            assertTrue(lines.get(lines.size() - 1).contains("HyperAir"));                       // Check that the last added line is our inserted customer
        }
    }

}
