package org.dandoy.dbpop;

import org.dandoy.DbPopUtils;
import org.dandoy.LocalCredentials;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

import static org.dandoy.test.SqlServerTests.assertCount;

@EnabledIf("org.dandoy.DbPopUtils#hasMssql")
public class DbPopTests {

    private static Connection targetConnection;

    @BeforeAll
    static void beforeAll() throws SQLException {
        targetConnection = LocalCredentials.from("mssql").createTargetConnection();
        DbPopUtils.prepareMssqlSource();
    }

    @AfterAll
    static void afterAll() throws SQLException {
        targetConnection.close();
    }

    @BeforeEach
    void setUp() {
        DbPopUtils.prepareMssqlTarget();
    }

    @Test
    void testPopulate() throws SQLException {
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Database database = Database.createDatabase(localCredentials.targetConnectionBuilder())) {
            Populator populator = Populator.createPopulator(database, new File("src/test/resources/mssql"));
            populator.load("base");
            assertCount(targetConnection, "customers", 3);
            assertCount(targetConnection, "invoices", 4);
            assertCount(targetConnection, "invoice_details", 7);
            assertCount(targetConnection, "products", 3);
        }
    }

    @Test
    void testDbPopMain() throws SQLException {
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Database database = Database.createDatabase(localCredentials.targetConnectionBuilder())) {
            Populator populator = Populator.createPopulator(database, new File("src/test/resources/mssql"));
            populator.load("base");
            assertCount(targetConnection, "customers", 3);
            assertCount(targetConnection, "invoices", 4);
            assertCount(targetConnection, "invoice_details", 7);
            assertCount(targetConnection, "products", 3);

            populator.load("extra");
            assertCount(targetConnection, "customers", 4);
            assertCount(targetConnection, "invoices", 4);
            assertCount(targetConnection, "invoice_details", 7);
            assertCount(targetConnection, "products", 3);

            populator.load("base");
            assertCount(targetConnection, "customers", 3);
            assertCount(targetConnection, "invoices", 4);
            assertCount(targetConnection, "invoice_details", 7);
            assertCount(targetConnection, "products", 3);
        }
    }
}
