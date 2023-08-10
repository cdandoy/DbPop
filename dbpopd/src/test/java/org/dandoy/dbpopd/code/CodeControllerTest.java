package org.dandoy.dbpopd.code;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.tests.mssql.DbPopContainerTest;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.download.DownloadController;
import org.dandoy.dbpopd.download.DownloadResponse;
import org.dandoy.dbpopd.populate.PopulateService;
import org.dandoy.dbpopd.utils.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * This test uses package private classes, so we can't move it out of this package.
 */
@DbPopContainerTest(source = true, target = true)
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

    @SuppressWarnings("SqlResolve")
    @Test
    void name() throws Exception {
        boolean codeAutoSave = configurationService.isCodeAutoSave();
        configurationService.setCodeAutoSave(false);
        try {
            {   // Download the code from the source database
                DownloadResult downloadResult = codeController.downloadSourceToFile();
                assertTrue(downloadResult.getCodeTypeCount("Stored Procedures") > 0);

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
                assertTrue(downloadResult.getCodeTypeCount("Stored Procedures") > 0);
            }

            {   // Upload to the target
                UploadResult uploadResult = codeController.uploadFileToTarget();
                assertNotNull(uploadResult.getFileExecution("SQL_STORED_PROCEDURE", "GetCustomers"));
                assertNotNull(uploadResult.getFileExecution("SQL_STORED_PROCEDURE", "GetInvoices"));
            }

            {   // Populate some data
                populateService.populate(List.of("base"));
            }

            {   // Change the code verify it gets downloaded
                changeDetector.getDatabaseChangeDetector().captureObjectSignatures();
                int sizeBefore = codeController.targetChanges().toList().size();
                try (Connection targetConnection = configurationService.createTargetConnection()) {
                    try (Statement statement = targetConnection.createStatement()) {
                        statement.execute("USE dbpop");
                        statement.execute("""
                                ALTER PROCEDURE GetCustomers @customer_id INT AS
                                BEGIN
                                    -- Coucou
                                    SELECT customer_id, customer_type_id, name
                                    FROM dbpop.dbo.customers
                                    WHERE customer_id = @customer_id + 1
                                END
                                """);
                    }
                }
                changeDetector.getDatabaseChangeDetector().compareObjectSignatures();
                List<CodeController.ChangeResponse> list = codeController.targetChanges().toList();
                int sizeAfter = list.size();
                assertEquals(sizeBefore + 1, sizeAfter);
                CodeController.ChangeResponse changeResponse = list.stream().filter(it -> it.objectIdentifier().tableName().getTable().equals("GetCustomers")).findFirst().orElseThrow();
                codeController.applyToFile(new CodeController.ObjectIdentifierResponse[]{
                        new CodeController.ObjectIdentifierResponse(
                                changeResponse.objectIdentifier().type(),
                                changeResponse.objectIdentifier().tableName(),
                                changeResponse.objectIdentifier().parent()
                        )
                });

                File file = new File(configurationService.getCodeDirectory(), "dbpop/dbo/SQL_STORED_PROCEDURE/GetCustomers.sql");
                assertTrue(IOUtils.toString(file).contains("Coucou"));

            }
        } finally {
            configurationService.setCodeAutoSave(codeAutoSave);
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

        changeDetector.getDatabaseChangeDetector().checkCodeChanges();
    }
}