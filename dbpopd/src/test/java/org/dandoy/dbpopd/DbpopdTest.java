package org.dandoy.dbpopd;

import io.micronaut.core.convert.ConversionService;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.FileUtils.readLines;
import static org.dandoy.dbpopd.utils.FileUtils.deleteRecursively;
import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
class DbpopdTest {

    @Inject
    DbpopdController dbpopdController;

    @Test
    void testUpload() {
        List<String> dataset = List.of("base", "invoices", "invoice_details");
        DbpopdService.PopulateResult result = dbpopdController.populate(dataset);
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
    void testDownload() throws IOException {
        File dir = new File("src/test/resources/config/datasets/test_dataset/");
        File file = new File(dir, "/master/dbo/customers.csv");
        if (dir.exists()) {
            deleteRecursively(dir);
        }

        DownloadRequest downloadRequest = ConversionService.SHARED
                .convertRequired(
                        Map.of(
                                "dataset", "test_dataset",
                                "catalog", "master",
                                "schema", "dbo",
                                "table", "customers",
                                "where", Map.of(
                                        "customer_id", 102
                                )
                        ),
                        DownloadRequest.class);
        assertNotNull(downloadRequest);

        dbpopdController.download(downloadRequest);

        assertTrue(file.exists());
        assertEquals(2, readLines(file, UTF_8).size());
        String content = FileUtils.readFileToString(file, UTF_8);
        assertTrue(content.contains("Crown"));
        assertFalse(content.contains("AirMethod"));

        deleteRecursively(dir);
    }
}
