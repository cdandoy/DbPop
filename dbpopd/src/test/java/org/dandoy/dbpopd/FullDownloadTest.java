package org.dandoy.dbpopd;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.dandoy.dbpop.database.Dependency;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpopd.download.DownloadController;
import org.dandoy.dbpopd.download.DownloadRequest;
import org.dandoy.dbpopd.download.DownloadResponse;
import org.dandoy.dbpopd.populate.PopulateResult;
import org.dandoy.dbpopd.populate.PopulateService;
import org.dandoy.dbpopd.utils.DbPopTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

@MicronautTest(environments = "temp-test")
public class FullDownloadTest {
    @Inject
    ConfigurationService configurationService;
    @Inject
    DownloadController downloadController;
    @Inject
    PopulateService populateService;
    @Inject
    CsvAssertionService csvAssertionService;

    @BeforeAll
    static void setUp() {
        DbPopTestUtils.setUp();
    }

    @Test
    void test() throws SQLException {
        try (Connection connection = configurationService.getTargetConnectionBuilder().createConnection()) {
            SqlExecutor.execute(
                    connection,
                    "/mssql/drop.sql",
                    "/mssql/create.sql"
            );
        }

        // source -> dataset
        DownloadResponse bulkStaticResponse = downloadController.bulkDownload(
                new DownloadController.DownloadBulkBody(
                        "static",
                        List.of(
                                new TableName("dbpop", "dbo", "customer_types"),
                                new TableName("dbpop", "dbo", "product_categories"),
                                new TableName("dbpop", "dbo", "customers"),
                                new TableName("dbpop", "dbo", "products")
                        )
                )
        );
        assertEquals(10, bulkStaticResponse.getRowCount());
        for (DownloadResponse.TableRowCount tableRowCount : bulkStaticResponse.getTableRowCounts()) {
            int expectedRowCount = switch (tableRowCount.getTableName().getTable()) {
                case "customer_types", "product_categories" -> 2;
                case "customers", "products" -> 3;
                default -> throw new RuntimeException();
            };
            assertEquals(expectedRowCount, tableRowCount.getRowCount());
        }

        DownloadResponse structuredDownloadResponse = downloadController.structuredDownload(
                new DownloadRequest()
                        .setDataset("base")
                        .setDependency(
                                new Dependency(
                                        new TableName("dbpop", "dbo", "invoices"),
                                        null,
                                        List.of(
                                                new Dependency(
                                                        new TableName("dbpop", "dbo", "customers"),
                                                        "invoices_customers_fk",
                                                        List.of(),
                                                        true,
                                                        true,
                                                        emptyList()
                                                ),
                                                new Dependency(
                                                        new TableName("dbpop", "dbo", "invoice_details"),
                                                        "invoice_details_invoices_fk",
                                                        List.of(),
                                                        true,
                                                        false,
                                                        emptyList()
                                                )
                                        ),
                                        true,
                                        true,
                                        emptyList()
                                )
                        )
                        .setQueryValues(emptyMap())
                        .setDryRun(false)
        );
        assertEquals(11, structuredDownloadResponse.getRowCount()); // 4 invoices, 7 invoice details
        assertEquals(2, structuredDownloadResponse.getRowsSkipped()); // 2 customers

        // Dataset -> target
        PopulateResult populateResult = populateService.populate(List.of("base"), true);
        assertEquals(21, populateResult.rows());

        // Add some data to target
        Connection connection = configurationService.getTargetDatabaseCache().getConnection();
        try (PreparedStatement preparedStatement = connection
                .prepareStatement("""
                        INSERT INTO dbpop.dbo.products(part_no, part_desc)
                        VALUES ('9999', 'NineNineNineNine');
                                                
                        UPDATE dbpop.dbo.products
                        SET part_desc = 'Circuit Playground Classique'
                        WHERE part_no = '3000'
                                                
                        UPDATE dbpop.dbo.invoice_details
                        SET product_id = 13
                        WHERE invoice_detail_id = 10001""")) {
            preparedStatement.executeUpdate();
            preparedStatement.getConnection().commit();
        }

        // Add a table
        try (Statement statement = connection.createStatement()) {
            statement.execute("");
            statement.execute("""
                    DROP TABLE IF EXISTS dbpop.dbo.test_new_table;
                    CREATE TABLE dbpop.dbo.test_new_table
                    (
                        id  INTEGER PRIMARY KEY IDENTITY,
                        txt VARCHAR(64)
                    );
                    INSERT INTO dbpop.dbo.test_new_table (txt) VALUES ('one')
                    """);
        }

        // Download back target -> source
        DownloadResponse downloadResponse = downloadController.downloadTarget(new DownloadController.DownloadTargetBody("base"));
        Assertions.assertEquals(23, downloadResponse.getRowCount());
        Assertions.assertEquals(0, downloadResponse.getRowsSkipped());
        csvAssertionService
                .csvAssertion("dbpop.dbo.products")
                .assertUnique("part_no")
                .assertExists(
                        List.of("part_no", "part_desc"),
                        List.of("3000", "Circuit Playground Classique"),
                        List.of("4561", "High Quality HQ Camera"),
                        List.of("5433", "Pogo Pin Probe Clip"),
                        List.of("9999", "NineNineNineNine")
                );
        csvAssertionService
                .csvAssertion("base", "dbpop.dbo.invoice_details")
                .assertUnique("invoice_detail_id")
                .assertExists(
                        List.of("invoice_id", "product_id"),
                        List.of("1001", "13"),
                        List.of("1001", "12"),
                        List.of("1002", "12"),
                        List.of("1002", "13"),
                        List.of("1003", "11"),
                        List.of("1003", "12"),
                        List.of("1003", "13")
                );
        csvAssertionService
                .csvAssertion("base", "dbpop.dbo.test_new_table")
                .assertUnique("id")
                .assertExists(
                        List.of("id", "txt"),
                        List.of("1", "one")
                );
    }
}
