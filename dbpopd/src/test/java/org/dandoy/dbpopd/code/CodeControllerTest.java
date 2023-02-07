package org.dandoy.dbpopd.code;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.utils.DbPopTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@MicronautTest(environments = "temp-test")
class CodeControllerTest {
    @Inject
    CodeController codeController;
    @Inject
    ConfigurationService configurationService;

    @BeforeAll
    static void setUp() {
        DbPopTestUtils.setUp();
    }

    @Test
    void name() {
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

        {   // Download from the target should not find anything newer
            DownloadResult downloadResult = codeController.downloadTargetToFile();
            assertEquals(0, downloadResult.getCodeTypeCounts().size());
        }
    }
}