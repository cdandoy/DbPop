package org.dandoy.dbpopd;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpop.tests.TestUtils;
import org.dandoy.dbpopd.populate.PopulateService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@MicronautTest(environments = "temp-test")
public class FullDownloadTest {
    private final ConfigurationService configurationService;
    private final DownloadController downloadController;
    private final PopulateService populateService;
    private final CsvAssertionService csvAssertionService;

    @BeforeAll
    static void beforeAll() {
        TestUtils.prepareTempDatasetDir();
    }

    public FullDownloadTest(
            ConfigurationService configurationService,
            DownloadController downloadController,
            PopulateService populateService,
            CsvAssertionService csvAssertionService
    ) {
        this.configurationService = configurationService;
        this.downloadController = downloadController;
        this.populateService = populateService;
        this.csvAssertionService = csvAssertionService;
    }

    @Test
    void test() throws SQLException {
        try (Connection connection = configurationService.getTargetConnectionBuilder().createConnection()) {
            SqlExecutor.execute(
                    connection,
                    "/mssql/drop_tables.sql",
                    "/mssql/create_tables.sql"
            );
        }

        populateService.populate(List.of("invoices", "invoice_details"), true);

        try (PreparedStatement preparedStatement = configurationService.getTargetDatabaseCache()
                .getConnection()
                .prepareStatement("""
                        INSERT INTO master.dbo.products(part_no, part_desc)
                        VALUES ('9999', 'NineNineNineNine');
                                                
                        UPDATE master.dbo.products
                        SET part_desc = 'Circuit Playground Classique'
                        WHERE part_no = '3000'
                                                
                        UPDATE master.dbo.invoice_details
                        SET product_id = 13
                        WHERE invoice_detail_id = 10001""")) {
            preparedStatement.executeUpdate();
            preparedStatement.getConnection().commit();
        }

        DownloadResponse downloadResponse = downloadController.downloadTarget(new DownloadController.DownloadTargetBody("invoice_details"));
        Assertions.assertEquals(14, downloadResponse.getRowCount());
        Assertions.assertEquals(0, downloadResponse.getRowsSkipped());
        csvAssertionService
                .csvAssertion("master.dbo.products")
                .assertUnique("part_no")
                .assertExists(
                        List.of("part_no", "part_desc"),
                        List.of("3000", "Circuit Playground Classique"),
                        List.of("4561", "High Quality HQ Camera"),
                        List.of("5433", "Pogo Pin Probe Clip"),
                        List.of("9999", "NineNineNineNine")
                );
        csvAssertionService
                .csvAssertion("invoice_details", "master.dbo.invoice_details")
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
    }
}
