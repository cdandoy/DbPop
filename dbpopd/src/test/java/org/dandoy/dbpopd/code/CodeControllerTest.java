package org.dandoy.dbpopd.code;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.dandoy.dbpopd.ConfigurationService;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@MicronautTest(environments = "temp-test")
class CodeControllerTest {
    @Inject
    CodeController codeController;
    @Inject
    ConfigurationService configurationService;
    @Inject
    ChangeDetector changeDetector;

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

        {   // Upload to the target a second time with one file updated
            System.out.println("Upload to the target a second time with one file updated");
            File file = new File("../files/temp/code/dbpop/dbo/SQL_STORED_PROCEDURE/GetCustomers.sql");
            String definition = IOUtils.readFully(file);
            try (BufferedWriter bufferedWriter = Files.newBufferedWriter(file.toPath())) {
                bufferedWriter.write(definition + "\n--minor change\n");
            }
            UploadResult uploadResult = codeController.uploadFileToTarget();
            assertEquals(1, uploadResult.fileExecutions().size());
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