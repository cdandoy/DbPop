package org.dandoy.dbpopd.code;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.download.DownloadController;
import org.dandoy.dbpopd.download.DownloadResponse;
import org.dandoy.dbpopd.populate.PopulateService;
import org.dandoy.dbpopd.utils.DbPopTestUtils;
import org.dandoy.dbpopd.utils.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@MicronautTest(environments = "temp-test")
class CodeControllerTest {
    @Inject
    DownloadController downloadController;
    @Inject
    CodeController codeController;
    @Inject
    ConfigurationService configurationService;
    @Inject
    ChangeDetector changeDetector;
    @Inject
    PopulateService populateService;

    @BeforeAll
    static void setUp() {
        DbPopTestUtils.setUp();
    }

    @SuppressWarnings("SqlResolve")
    @Test
    void name() throws SQLException, InterruptedException, IOException {
        {   // Download from the source
            DownloadResult downloadResult = codeController.downloadSourceToFile();
            assertEquals(2, downloadResult.getCodeTypeCount("Stored Procedures"));

            DownloadResponse downloadResponse = downloadController.bulkDownload(new DownloadController.DownloadBulkBody("base", List.of(
                    new TableName("dbpop", "dbo", "customer_types"),
                    new TableName("dbpop", "dbo", "customers"),
                    new TableName("dbpop", "dbo", "deliveries"),
                    new TableName("dbpop", "dbo", "invoice_details"),
                    new TableName("dbpop", "dbo", "invoices"),
                    new TableName("dbpop", "dbo", "order_details"),
                    new TableName("dbpop", "dbo", "order_types"),
                    new TableName("dbpop", "dbo", "orders"),
                    new TableName("dbpop", "dbo", "product_categories"),
                    new TableName("dbpop", "dbo", "products")
            )));
            assertNotNull(downloadResponse);
        }

        {   // Downloading again from the source must override
            DownloadResult downloadResult = codeController.downloadSourceToFile();
            assertEquals(2, downloadResult.getCodeTypeCount("Stored Procedures"));
        }

        {   // Upload to the target
            UploadResult uploadResult = codeController.uploadFileToTarget();
            assertNotNull(uploadResult.getFileExecution("SQL_STORED_PROCEDURE", "GetCustomers"));
            assertNotNull(uploadResult.getFileExecution("SQL_STORED_PROCEDURE", "GetInvoices"));
        }

        {   // Populate some data
            populateService.populate(List.of("base"));
        }

        {   // Upload to the target a second time with files updated
            Thread.sleep(200);
            File[] files = {
                    new File("../files/temp/code/dbpop/dbo/SQL_STORED_PROCEDURE/GetCustomers.sql"),
                    new File("../files/temp/code/dbpop/dbo/USER_TABLE/customers.sql"),
            };
            for (File file : files) {
                String definition = IOUtils.toString(file);
                try (BufferedWriter bufferedWriter = Files.newBufferedWriter(file.toPath())) {
                    bufferedWriter.write(definition + "\n--minor change\n");
                }
            }
            UploadResult uploadResult = codeController.uploadFileToTarget();
            assertEquals(2, uploadResult.fileExecutions().size());
        }

        {   // Download from the target should not find anything newer
            DownloadResult downloadResult = codeController.downloadTargetToFile();
            assertEquals(0, downloadResult.getCodeTypeCounts().size());
        }

        {   // Download from the target after changing code
            Thread.sleep(1100); // Give time to the developer to change that sproc
            try (Connection targetConnection = configurationService.createTargetConnection()) {
                try (Statement statement = targetConnection.createStatement()) {
                    statement.execute("USE dbpop");
                    statement.execute("""
                            ALTER PROCEDURE GetCustomers @customer_id INT AS
                            BEGIN
                                SELECT customer_id, customer_type_id, name
                                FROM dbpop.dbo.customers
                                WHERE customer_id = @customer_id + 1
                            END
                            """);
                }
            }
            DownloadResult downloadResult = codeController.downloadTargetToFile();
            assertEquals(1, downloadResult.getCodeTypeCounts().size());
        }
    }

    @Test
    void testChangeDetector() throws SQLException, InterruptedException {
        // Download from the source
        codeController.downloadSourceToFile();
        codeController.uploadFileToTarget();
        {   // Download from the target after changing code
            Thread.sleep(1100); // Give time to the developer to change that sproc
            try (Connection targetConnection = configurationService.createTargetConnection()) {
                try (Statement statement = targetConnection.createStatement()) {
                    statement.execute("USE dbpop");
                    //noinspection SqlResolve
                    statement.execute("""
                            ALTER PROCEDURE GetCustomers @customer_id INT AS
                            BEGIN
                                SELECT customer_id, customer_type_id, name
                                FROM dbpop.dbo.customers
                                WHERE customer_id = @customer_id
                            END
                            """);
                }
            }
        }

        changeDetector.checkCodeChanges();
    }
}