package org.dandoy.dbpop.download;

import org.dandoy.LocalCredentials;
import org.dandoy.DbPopUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.tests.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.util.List;
import java.util.Set;

import static org.dandoy.DbPopUtils.invoiceDetails;
import static org.dandoy.DbPopUtils.invoices;

@EnabledIf("org.dandoy.DbPopUtils#hasMssql")
class TableDownloaderTest {
    private static final File TEST_DIR = new File("src/test/resources/mssql/download");

    @BeforeAll
    static void beforeAll() {
        DbPopUtils.prepareMssqlSource();
    }

    @BeforeEach
    void setUp() {
        DbPopUtils.prepareMssqlTarget();
    }

    @Test
    void testByPrimaryKey() {
        TestUtils.delete(TEST_DIR);

        File datasetsDirectory = new File("src/test/resources/mssql");
        LocalCredentials localCredentials = LocalCredentials.from("mssql");

        try (Database sourceDatabase = Database.createDatabase(localCredentials.sourceConnectionBuilder())) {
            Table table = sourceDatabase.getTable(invoices);
            try (TableDownloader tableDownloader = TableDownloader.builder()
                    .setDatabase(sourceDatabase)
                    .setDatasetsDirectory(datasetsDirectory)
                    .setDataset("download")
                    .setTableName(invoices)
                    .setFilteredColumns(table.getPrimaryKey().columns())
                    .setExecutionMode(ExecutionMode.SAVE)
                    .build()) {

                Set<List<Object>> pks = Set.of(List.of(1001), List.of(1002));
                tableDownloader.download(pks);
            }
        }
        TestUtils.delete(TEST_DIR);
    }

    @Test
    void testByForeignKey() {
        TestUtils.delete(TEST_DIR);

        File datasetsDirectory = new File("src/test/resources/mssql");
        LocalCredentials localCredentials = LocalCredentials.from("mssql");

        try (Database database = Database.createDatabase(localCredentials.sourceConnectionBuilder())) {
            Table table = database.getTable(invoiceDetails);
            List<String> fkColumns = table.getForeignKeys().stream()
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
        TestUtils.delete(TEST_DIR);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testFull() {
        TestUtils.delete(TEST_DIR);
        File datasetsDirectory = new File("src/test/resources/mssql");
        new File(datasetsDirectory, "download/master/dbo/invoices.csv").delete();
        LocalCredentials localCredentials = LocalCredentials.from("mssql");

        try (Database database = Database.createDatabase(localCredentials.sourceConnectionBuilder())) {
            try (TableDownloader tableDownloader = TableDownloader.builder()
                    .setDatabase(database)
                    .setDatasetsDirectory(datasetsDirectory)
                    .setDataset("download")
                    .setTableName(invoices)
                    .build()) {
                tableDownloader.download();
            }
        }
        TestUtils.delete(TEST_DIR);
    }
}