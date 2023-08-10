package org.dandoy.dbpopd.mssql;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.dandoy.dbpop.database.Dependency;
import org.dandoy.dbpop.database.Query;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.tests.TableAssertion;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.CsvAssertionService;
import org.dandoy.dbpopd.download.DownloadController;
import org.dandoy.dbpopd.download.DownloadRequest;
import org.dandoy.dbpopd.download.DownloadResponse;
import org.dandoy.dbpop.tests.mssql.DbPopContainerTest;
import org.dandoy.dbpopd.populate.PopulateService;
import org.junit.jupiter.api.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DbPopContainerTest(source = true, target = true, withTargetTables = true)
@MicronautTest(environments = "temp-test")
public class TestOtherDataset {
    @Inject
    DownloadController downloadController;
    @Inject
    PopulateService populateService;
    @Inject
    CsvAssertionService csvAssertionService;
    @Inject
    ConfigurationService configurationService;

    @Test
    void testBulk() throws IOException, SQLException {
        downloadController.bulkDownload(
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
        try (FileOutputStream outputStream = new FileOutputStream("../files/temp/datasets/base/dbpop/dbo/invoices.csv")) {
            String csv = """
                    invoice_id,customer_id,invoice_date
                    1001,101,2022-01-01 00:00:00
                    """;
            IOUtils.write(csv, outputStream, UTF_8);
        }

        {
            DownloadResponse downloadResponse = downloadController.bulkDownload(
                    new DownloadController.DownloadBulkBody(
                            "other",
                            List.of(
                                    new TableName("dbpop", "dbo", "invoices")
                            )
                    )
            );
            assertEquals(3, downloadResponse.getRowCount());
            assertEquals(1, downloadResponse.getRowsSkipped());
        }

        {   // Check the downloaded rows
            csvAssertionService.csvAssertion("other", "dbpop.dbo.invoices")
                    .assertRowCount(3)
                    .assertExists(
                            List.of("invoice_id"),
                            List.of("1002"),
                            List.of("1003"),
                            List.of("1004")
                    );
        }

        {
            populateService.populate(List.of("other"), true);
            try (Connection targetConnection = configurationService.createTargetConnection()) {
                new TableAssertion(targetConnection, "dbpop", "dbo", "invoices")
                        .assertRowCount(4)
                        .assertExists(
                                List.of("invoice_id"),
                                List.of("1001"),
                                List.of("1002"),
                                List.of("1003"),
                                List.of("1004")
                        );
            }
        }
    }

    @Test
    void testStructured() throws SQLException {
        downloadController.bulkDownload(
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

        {   // Download invoice 1001 in base
            DownloadResponse structuredDownloadResponse = downloadController.structuredDownload(
                    createInvoiceStructuredDownloadRequest("base", new Query("invoice_id", "1001"))
            );
            assertEquals(1, structuredDownloadResponse.getRowCount());
        }

        {   // Check the downloaded rows in base
            csvAssertionService.csvAssertion("base", "dbpop.dbo.invoices")
                    .assertRowCount(1)
                    .assertExists(
                            List.of("invoice_id"),
                            List.of("1001")
                    );
        }

        // Download invoice 1002, 1003, 1004 in other
        for (String invoiceId : new String[]{"1002", "1003", "1004"}) {
            DownloadResponse structuredDownloadResponse = downloadController.structuredDownload(createInvoiceStructuredDownloadRequest("other", new Query("invoice_id", invoiceId)));
            assertEquals(1, structuredDownloadResponse.getRowCount());
        }

        {   // Check the downloaded rows in base
            csvAssertionService.csvAssertion("other", "dbpop.dbo.invoices")
                    .assertRowCount(3)
                    .assertExists(
                            List.of("invoice_id"),
                            List.of("1002"),
                            List.of("1003"),
                            List.of("1004")
                    );
        }

        {   // Populate others, should merge with base
            populateService.populate(List.of("other"), true);
        }

        {   // Check the row counts
            try (Connection targetConnection = configurationService.createTargetConnection()) {
                new TableAssertion(targetConnection, "dbpop", "dbo", "invoices")
                        .assertRowCount(4)
                        .assertExists(
                                List.of("invoice_id"),
                                List.of("1001"),
                                List.of("1002"),
                                List.of("1003"),
                                List.of("1004")
                        );
            }
        }
    }

    private static DownloadRequest createInvoiceStructuredDownloadRequest(String dataset, Query invoiceQuery) {
        return new DownloadRequest()
                .setDataset(dataset)
                .setDependency(
                        new Dependency(
                                new TableName("dbpop", "dbo", "invoices"),
                                null,
                                List.of(),
                                true,
                                true,
                                List.of(invoiceQuery)
                        )
                )
                .setQueryValues(emptyMap())
                .setDryRun(false);
    }
}
