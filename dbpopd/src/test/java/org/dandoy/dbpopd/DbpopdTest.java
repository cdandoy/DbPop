package org.dandoy.dbpopd;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.database.Dependency;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.commons.io.FileUtils.readLines;
import static org.dandoy.dbpop.utils.FileUtils.deleteRecursively;
import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
class DbpopdTest {
    public static final TableName invoices = new TableName("master", "dbo", "invoices");
    public static final TableName customers = new TableName("master", "dbo", "customers");
    public static final TableName invoiceDetails = new TableName("master", "dbo", "invoice_details");
    @Inject
    PopulateController populateController;
    @Inject
    DownloadController downloadController;
    @Inject
    ConfigurationService configurationService;

    @BeforeEach
    void setUp() throws SQLException {
        try (Connection connection = configurationService.getTargetConnectionBuilder().createConnection()) {
            SqlExecutor.execute(
                    connection,
                    "/mssql/drop_tables.sql",
                    "/mssql/create_tables.sql"
            );
        }

        populateController.resetPopulatorHolder();
    }

    @Test
    void testUpload() {
        List<String> dataset = List.of("base", "invoices", "invoice_details");
        PopulateController.PopulateResult result = populateController.populate(dataset);
        /*
            customers.csv         3
            invoice_details.csv   7
            invoices.csv          4
            products.csv          3
            total                 17
         */
        assertEquals(17, result.rows());
    }

    @Test
    void testCount() throws SQLException {
        long t0 = System.currentTimeMillis();
        populateController.populate(List.of("invoices", "invoice_details"));
        long t1 = System.currentTimeMillis();

        DownloadRequest downloadRequest = createInvoiceDownloadRequest("static", true);
        DownloadResponse downloadResponse = downloadController.download(downloadRequest);
        long t2 = System.currentTimeMillis();
        System.out.printf("populate: %dms, download: %dms%n", t1 - t0, t2 - t1);


        assertEquals(
                4,
                downloadResponse
                        .getTableRowCounts().stream()
                        .filter(it -> invoices.equals(it.getTableName()))
                        .findFirst()
                        .orElseThrow()
                        .getRowCount()
        );
    }

    @Test
    @Disabled("Rewrite this test")
    void testDownload() throws IOException, SQLException {
        populateController.populate(singletonList("customers_1000"));

        File dir = new File("src/test/resources/config/datasets/test_dataset/");
        File file = new File(dir, "/master/dbo/customers.csv");
        if (dir.exists()) {
            deleteRecursively(dir);
        }

        DownloadRequest downloadRequest = createInvoiceDownloadRequest("static", false);
        assertNotNull(downloadRequest);

        downloadController.download(downloadRequest);

        assertTrue(file.exists());
        assertEquals(2, readLines(file, UTF_8).size());
        String content = FileUtils.readFileToString(file, UTF_8);
        assertTrue(content.contains("Ontel"));
        assertFalse(content.contains("Arris"));

        deleteRecursively(dir);
    }

    @SuppressWarnings("SameParameterValue")
    private static DownloadRequest createInvoiceDownloadRequest(String dataset, boolean dryRun) {
        return new DownloadRequest()
                .setDataset(dataset)
                .setDependency(
                        new Dependency(
                                invoices,
                                null,
                                List.of(
                                        new Dependency(
                                                customers,
                                                "invoices_customers_fk",
                                                List.of(),
                                                true,
                                                true
                                        ),
                                        new Dependency(
                                                invoiceDetails,
                                                "invoice_details_invoices_fk",
                                                List.of(),
                                                false,
                                                false
                                        )
                                ),
                                true,
                                true
                        )
                )
                .setQueryValues(emptyMap())
                .setDryRun(dryRun);
    }
}