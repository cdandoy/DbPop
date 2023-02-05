package org.dandoy.dbpopd.code;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.utils.DbPopTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@MicronautTest(environments = "temp-test")
class CodeControllerTest {
    @Inject
    CodeController codeController;
    @Inject
    ConfigurationService configurationService;
    @Inject
    DbPopTestUtils dbPopTestUtils;

    @BeforeEach
    void setUp() throws SQLException {
        dbPopTestUtils.setUp();
    }

    @Test
    void name() {
        {
            DownloadResult downloadResult = codeController.downloadSourceToFile();
            assertEquals(2, downloadResult.getCodeTypeCount("Stored Procedures"));
        }

        {
            DownloadResult downloadResult = codeController.downloadSourceToFile();
            assertEquals(0, downloadResult.getCodeTypeCount("Stored Procedures"));
        }

        {
            UploadResult uploadResult = codeController.uploadFileToTarget();
            assertNotNull(uploadResult.getFileExecution("SQL_STORED_PROCEDURE", "GetCustomers"));
            assertNotNull(uploadResult.getFileExecution("SQL_STORED_PROCEDURE", "GetInvoices"));
        }

        {
            DownloadResult downloadResult = codeController.downloadTargetToFile();
            assertEquals(0, downloadResult.getCodeTypeCounts().size());
        }
    }
}