package org.dandoy.dbpop;

import org.dandoy.LocalCredentials;
import org.dandoy.TestUtils;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.sql.Connection;
import java.sql.SQLException;

import static org.dandoy.test.SqlServerTests.assertCount;

@EnabledIf("org.dandoy.TestUtils#hasMssql")
public class DbPopTests {

    private static Connection targetConnection;

    @BeforeAll
    static void beforeAll() throws SQLException {
        targetConnection = LocalCredentials.from("mssql").createTargetConnection();
        TestUtils.prepareMssqlSource();
    }

    @AfterAll
    static void afterAll() throws SQLException {
        targetConnection.close();
    }

    @BeforeEach
    void setUp() {
        TestUtils.prepareMssqlTarget();
    }

    @Test
    void testPopulate() throws SQLException {
        Populator populator = LocalCredentials.mssqlPopulator("src/test/resources/mssql");
        populator.load("invoices");
        assertCount(targetConnection, "customers", 3);
        assertCount(targetConnection, "invoices", 4);
        assertCount(targetConnection, "invoice_details", 7);
        assertCount(targetConnection, "products", 3);
    }

    @Test
    void testDbPopMain() throws SQLException {
        Populator populator = LocalCredentials.mssqlPopulator("src/test/resources/mssql");
        populator.load("base");
        assertCount(targetConnection, "customers", 3);
        assertCount(targetConnection, "invoices", 0);
        assertCount(targetConnection, "invoice_details", 0);
        assertCount(targetConnection, "products", 3);

        populator.load("base", "invoices");
        assertCount(targetConnection, "customers", 3);
        assertCount(targetConnection, "invoices", 4);
        assertCount(targetConnection, "invoice_details", 7);
        assertCount(targetConnection, "products", 3);

        populator.load("base");
        assertCount(targetConnection, "customers", 3);
        assertCount(targetConnection, "invoices", 0);
        assertCount(targetConnection, "invoice_details", 0);
        assertCount(targetConnection, "products", 3);
    }
}
