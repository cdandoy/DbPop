package org.dandoy.dbpop.download;

import org.dandoy.LocalCredentials;
import org.dandoy.TestUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

@EnabledIf("org.dandoy.TestUtils#hasSqlServer")
class TableDownloaderTest {
    private static final File TEST_DIR = new File("src/test/resources/mssql/download");

    @Test
    void testByPrimaryKey() throws SQLException {
        TestUtils.delete(TEST_DIR);

        File datasetsDirectory = new File("src/test/resources/mssql");
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Populator populator = localCredentials.populator()
                .setDirectory(datasetsDirectory)
                .build()) {
            populator.load("invoices");
        }

        try (Connection connection = localCredentials.createConnection()) {
            Database database = Database.createDatabase(connection);
            String dataset = "download";
            TableName tableName = new TableName("master", "dbo", "invoices");
            Table table = database.getTable(tableName);
            try (TableDownloader tableDownloader = TableDownloader.builder()
                    .setDatabase(database)
                    .setDatasetsDirectory(datasetsDirectory)
                    .setDataset(dataset)
                    .setTableName(tableName)
                    .setFilteredColumns(table.primaryKey().columns())
                    .build()) {

                Set<List<Object>> pks = Set.of(List.of(1001), List.of(1002));
                tableDownloader.download(pks);
            }
        }
        TestUtils.delete(TEST_DIR);
    }

    @Test
    void testByForeignKey() throws SQLException {
        TestUtils.delete(TEST_DIR);

        File datasetsDirectory = new File("src/test/resources/mssql");
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Populator populator = localCredentials.populator()
                .setDirectory(datasetsDirectory)
                .build()) {
            populator.load("invoices");
        }

        try (Connection connection = localCredentials.createConnection()) {
            Database database = Database.createDatabase(connection);
            String dataset = "download";
            TableName invoices = new TableName("master", "dbo", "invoices");
            TableName invoiceDetails = new TableName("master", "dbo", "invoice_details");
            Table table = database.getTable(invoiceDetails);
            List<String> fkColumns = table.foreignKeys().stream()
                    .filter(it -> it.getPkTableName().equals(invoices)).findFirst().orElseThrow()
                    .getFkColumns();
            try (TableDownloader tableDownloader = TableDownloader.builder()
                    .setDatabase(database)
                    .setDatasetsDirectory(datasetsDirectory)
                    .setDataset(dataset)
                    .setTableName(invoiceDetails)
                    .setFilteredColumns(fkColumns)
                    .build()) {

                Set<List<Object>> pks = Set.of(List.of(1001), List.of(1002));
                tableDownloader.download(pks);
            }
        }
        TestUtils.delete(TEST_DIR);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testFull() throws SQLException {
        TestUtils.delete(TEST_DIR);
        File datasetsDirectory = new File("src/test/resources/mssql");
        new File(datasetsDirectory, "download/master/dbo/invoices.csv").delete();
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Populator populator = localCredentials.populator()
                .setDirectory(datasetsDirectory)
                .build()) {
            populator.load("invoices");
        }

        try (Connection connection = localCredentials.createConnection()) {
            Database database = Database.createDatabase(connection);
            String dataset = "download";
            try (TableDownloader tableDownloader = TableDownloader.builder()
                    .setDatabase(database)
                    .setDatasetsDirectory(datasetsDirectory)
                    .setDataset(dataset)
                    .setTableName(new TableName("master", "dbo", "invoices"))
                    .build()) {
                tableDownloader.download();
            }
        }
        TestUtils.delete(TEST_DIR);
    }
}