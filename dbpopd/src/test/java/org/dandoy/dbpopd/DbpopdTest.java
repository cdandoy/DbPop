package org.dandoy.dbpopd;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.database.Dependency;
import org.dandoy.dbpop.database.ForeignKey;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpopd.populate.PopulateService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.*;
import static org.apache.commons.io.FileUtils.readLines;
import static org.dandoy.dbpop.utils.FileUtils.deleteRecursively;
import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
class DbpopdTest {
    public static final TableName invoices = new TableName("master", "dbo", "invoices");
    public static final TableName customers = new TableName("master", "dbo", "customers");
    public static final TableName invoiceDetails = new TableName("master", "dbo", "invoice_details");
    @Inject
    PopulateService populateService;
    @Inject
    DownloadController downloadController;
    @Inject
    DatabaseController databaseController;
    @Inject
    DatabaseVfksController databaseVfksController;
    @Inject
    ConfigurationService configurationService;

    @BeforeEach
    void setUp() throws SQLException {
        try (Connection connection = configurationService.getSourceConnectionBuilder().createConnection()) {
            SqlExecutor.execute(
                    connection,
                    "/mssql/drop_tables.sql",
                    "/mssql/create_tables.sql",
                    "/mssql/insert_data.sql"
            );
        }
        try (Connection connection = configurationService.getTargetConnectionBuilder().createConnection()) {
            SqlExecutor.execute(
                    connection,
                    "/mssql/drop_tables.sql",
                    "/mssql/create_tables.sql"
            );
        }
    }

    @Test
    void testUpload() {
        List<String> dataset = List.of("base", "invoices", "invoice_details");
        PopulateService.PopulateResult result = populateService.populate(dataset, true);
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
    void testCount() {
        long t0 = System.currentTimeMillis();
        populateService.populate(List.of("invoices", "invoice_details"));
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
    void testDownload() throws IOException {
        populateService.populate(singletonList("customers_1000"));

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
                                                true,
                                                emptyList()
                                        ),
                                        new Dependency(
                                                invoiceDetails,
                                                "invoice_details_invoices_fk",
                                                List.of(),
                                                false,
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
                .setDryRun(dryRun);
    }

    @SuppressWarnings("ThrowFromFinallyBlock")
    @Test
    void testVfk() {
        File file = new File(configurationService.getConfigurationDir(), "vfk.json");
        if (!file.delete() && file.exists()) throw new RuntimeException();

        try {
            // Start with 0 FKs
            assertEquals(0, databaseVfksController.getVirtualForeignKeys().size());

            // Add one, we must have one
            databaseVfksController.postVirtualForeignKey(
                    new ForeignKey(
                            "invoices_invoices_details_vfk",
                            "test1",
                            invoices,
                            List.of("invoice_id"),
                            invoiceDetails,
                            List.of("invoice_id")
                    )
            );
            List<ForeignKey> vfks = databaseVfksController.getVirtualForeignKeys();
            assertEquals(1, vfks.size());

            ForeignKey vfk = vfks.get(0);
            assertEquals("invoices_invoices_details_vfk", vfk.getName());
            assertEquals("test1", vfk.getConstraintDef());

            // Change it
            databaseVfksController.postVirtualForeignKey(
                    new ForeignKey(
                            "invoices_invoices_details_vfk",
                            "test2",
                            vfk.getPkTableName(),
                            vfk.getPkColumns(),
                            vfk.getFkTableName(),
                            vfk.getFkColumns()
                    )
            );

            // And verify
            List<ForeignKey> vfks2 = databaseVfksController.getVirtualForeignKeys();
            assertEquals(1, vfks2.size());

            ForeignKey vfk2 = vfks2.get(0);
            assertEquals("invoices_invoices_details_vfk", vfk2.getName());
            assertEquals("test2", vfk2.getConstraintDef());

            databaseVfksController.deleteVirtualForeignKey(
                    new ForeignKey(
                            "invoices_invoices_details_vfk",
                            "test2",
                            vfk.getPkTableName(),
                            vfk.getPkColumns(),
                            vfk.getFkTableName(),
                            vfk.getFkColumns()
                    )
            );
            assertEquals(0, databaseVfksController.getVirtualForeignKeys().size());
        } finally {
            if (!file.delete()) throw new RuntimeException();
        }
    }
}
