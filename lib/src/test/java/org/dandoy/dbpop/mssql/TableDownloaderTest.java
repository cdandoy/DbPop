package org.dandoy.dbpop.mssql;

import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.download.ExecutionMode;
import org.dandoy.dbpop.download.TableDownloader;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpop.tests.TestUtils;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static org.dandoy.dbpop.mssql.MsSqlTestUtils.invoiceDetails;
import static org.dandoy.dbpop.mssql.MsSqlTestUtils.invoices;

@Testcontainers
class TableDownloaderTest {
    private static final File TEST_DIR = new File("src/test/resources/mssql/download");
    @Container
    @SuppressWarnings({"rawtypes", "resource"})
    public static MSSQLServerContainer mssqlSource = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:latest").acceptLicense();
    private static DatabaseProxy sourceDatabase;

    @BeforeAll
    static void beforeAll() throws SQLException {
        ConnectionBuilder sourceConnectionBuilder = new UrlConnectionBuilder(
                mssqlSource.getJdbcUrl(),
                mssqlSource.getUsername(),
                mssqlSource.getPassword()
        );
        sourceDatabase = Database.createDatabase(sourceConnectionBuilder);
        Connection connection = sourceDatabase.getConnection();
        SqlExecutor.execute(connection,
                "/mssql/createdb.sql",
                "/mssql/create.sql",
                "/mssql/insert_data.sql"
        );
    }

    @AfterAll
    static void afterAll() {
        sourceDatabase.close();
    }

    @Test
    void testByPrimaryKey() {
        TestUtils.delete(TEST_DIR);

        File datasetsDirectory = new File("src/test/resources/mssql");

        Table table = sourceDatabase.getTable(invoices);
        try (TableDownloader tableDownloader = TableDownloader.builder()
                .setDatabase(sourceDatabase)
                .setDatasetsDirectory(datasetsDirectory)
                .setDataset("download")
                .setTableName(invoices)
                .setFilteredColumns(table.getPrimaryKey().getColumns())
                .setExecutionMode(ExecutionMode.SAVE)
                .build()) {

            Set<List<Object>> pks = Set.of(List.of(1001), List.of(1002));
            tableDownloader.download(pks);
        }
        TestUtils.delete(TEST_DIR);
    }

    @Test
    void testByForeignKey() {
        TestUtils.delete(TEST_DIR);

        File datasetsDirectory = new File("src/test/resources/mssql");

        Table table = sourceDatabase.getTable(invoiceDetails);
        List<String> fkColumns = table.getForeignKeys().stream()
                .filter(it -> it.getPkTableName().equals(invoices)).findFirst().orElseThrow()
                .getFkColumns();
        try (TableDownloader tableDownloader = TableDownloader.builder()
                .setDatabase(sourceDatabase)
                .setDatasetsDirectory(datasetsDirectory)
                .setDataset("download")
                .setTableName(invoiceDetails)
                .setFilteredColumns(fkColumns)
                .setExecutionMode(ExecutionMode.SAVE)
                .build()) {

            Set<List<Object>> pks = Set.of(List.of(1001), List.of(1002));
            tableDownloader.download(pks);
        }
        TestUtils.delete(TEST_DIR);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testFull() {
        TestUtils.delete(TEST_DIR);
        File datasetsDirectory = new File("src/test/resources/mssql");
        new File(datasetsDirectory, "download/master/dbo/invoices.csv").delete();

        try (TableDownloader tableDownloader = TableDownloader.builder()
                .setDatabase(sourceDatabase)
                .setDatasetsDirectory(datasetsDirectory)
                .setDataset("download")
                .setTableName(invoices)
                .build()) {
            tableDownloader.download();
        }
        TestUtils.delete(TEST_DIR);
    }
}