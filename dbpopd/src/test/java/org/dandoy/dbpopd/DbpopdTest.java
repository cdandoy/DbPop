package org.dandoy.dbpopd;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.dandoy.dbpop.database.ForeignKey;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpop.tests.TestUtils;
import org.dandoy.dbpopd.download.DownloadController;
import org.dandoy.dbpopd.populate.PopulateService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@MicronautTest(environments = "temp-test")
class DbpopdTest {
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
        TestUtils.prepareTempConfigDir();
        try (Connection connection = configurationService.getSourceConnectionBuilder().createConnection()) {
            SqlExecutor.execute(
                    connection,
                    "/mssql/drop.sql",
                    "/mssql/create.sql",
                    "/mssql/insert_data.sql"
            );
        }
        try (Connection connection = configurationService.getSourceConnectionBuilder().createConnection()) {
            SqlExecutor.execute(
                    connection,
                    "/mssql/drop.sql",
                    "/mssql/create.sql"
            );
        }
    }

    @AfterEach
    void tearDown() {
        TestUtils.deleteTempConfigDir();
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
                            new TableName("dbpop", "dbo", "invoices"),
                            List.of("invoice_id"),
                            new TableName("dbpop", "dbo", "invoice_details"),
                            List.of("product_id")
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
