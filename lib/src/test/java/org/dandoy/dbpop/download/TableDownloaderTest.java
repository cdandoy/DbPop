package org.dandoy.dbpop.download;

import org.dandoy.LocalCredentials;
import org.dandoy.TestUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static org.dandoy.TestUtils.invoiceDetails;
import static org.dandoy.TestUtils.invoices;

@EnabledIf("org.dandoy.TestUtils#hasMssql")
class TableDownloaderTest {
    private static final File TEST_DIR = new File("src/test/resources/mssql/download");

    @BeforeAll
    static void beforeAll() {
        TestUtils.prepareMssqlSource();
    }

    @BeforeEach
    void setUp() {
        TestUtils.prepareMssqlTarget();
    }

    @Test
    void testByPrimaryKey() throws SQLException {
        TestUtils.delete(TEST_DIR);

        File datasetsDirectory = new File("src/test/resources/mssql");
        LocalCredentials localCredentials = LocalCredentials.from("mssql");

        try (Connection sourceConnection = localCredentials.createSourceConnection()) {
            try (Database sourceDatabase = Database.createDatabase(sourceConnection)) {
                Table table = sourceDatabase.getTable(invoices);
                try (TableDownloader tableDownloader = TableDownloader.builder()
                        .setDatabase(sourceDatabase)
                        .setDatasetsDirectory(datasetsDirectory)
                        .setDataset("download")
                        .setTableName(invoices)
                        .setFilteredColumns(table.primaryKey().columns())
                        .setExecutionMode(ExecutionMode.SAVE)
                        .build()) {

                    Set<List<Object>> pks = Set.of(List.of(1001), List.of(1002));
                    tableDownloader.download(pks);
                }
            }
        }
        TestUtils.delete(TEST_DIR);
    }

    @Test
    void testByForeignKey() throws SQLException {
        TestUtils.delete(TEST_DIR);

        File datasetsDirectory = new File("src/test/resources/mssql");
        LocalCredentials localCredentials = LocalCredentials.from("mssql");

        try (Connection sourceConnection = localCredentials.createSourceConnection()) {
            try (Database database = Database.createDatabase(sourceConnection)) {
                Table table = database.getTable(invoiceDetails);
                List<String> fkColumns = table.foreignKeys().stream()
                        .filter(it -> it.getPkTableName().equals(invoices)).findFirst().orElseThrow()
                        .getFkColumns();
                try (TableDownloader tableDownloader = TableDownloader.builder()
                        .setDatabase(database)
                        .setDatasetsDirectory(datasetsDirectory)
                        .setDataset("download")
                        .setTableName(invoiceDetails)
                        .setFilteredColumns(fkColumns)
                        .setExecutionMode(ExecutionMode.SAVE)
                        .build()) {

                    Set<List<Object>> pks = Set.of(List.of(1001), List.of(1002));
                    tableDownloader.download(pks);
                }
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

        try (Connection sourceConnection = localCredentials.createSourceConnection()) {
            try (Database database = Database.createDatabase(sourceConnection)) {
                try (TableDownloader tableDownloader = TableDownloader.builder()
                        .setDatabase(database)
                        .setDatasetsDirectory(datasetsDirectory)
                        .setDataset("download")
                        .setTableName(invoices)
                        .build()) {
                    tableDownloader.download();
                }
            }
        }
        TestUtils.delete(TEST_DIR);
    }
}