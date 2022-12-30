package org.dandoy.dbpop;

import org.dandoy.LocalCredentials;
import org.dandoy.TestUtils;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.sql.Connection;
import java.sql.SQLException;

import static org.dandoy.test.SqlServerTests.assertCount;

@EnabledIf("org.dandoy.TestUtils#hasMssql")
public class DbPopTests {
    @BeforeAll
    static void beforeAll() {
        TestUtils.prepareMssqlSource();
    }

    @BeforeEach
    void setUp() {
        TestUtils.prepareMssqlTarget();
    }

    @Test
    void testPopulate() throws SQLException {
        try (Populator populator = LocalCredentials
                .mssqlPopulator()
                .setDirectory("src/test/resources/mssql")
                .build()) {
            try (Connection targetConnection = populator.createTargetConnection()) {
                populator.load("invoices");
                assertCount(targetConnection, "customers", 3);
                assertCount(targetConnection, "invoices", 4);
                assertCount(targetConnection, "invoice_details", 7);
                assertCount(targetConnection, "products", 3);
            }
        }
    }

    @Test
    void testDbPopMain() throws SQLException {
        try (Populator populator = LocalCredentials
                .mssqlPopulator()
                .setDirectory("src/test/resources/mssql")
                .build()) {
            try (Connection connection = populator.createTargetConnection()) {
                populator.load("base");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 0);
                assertCount(connection, "invoice_details", 0);
                assertCount(connection, "products", 3);

                populator.load("base", "invoices");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 4);
                assertCount(connection, "invoice_details", 7);
                assertCount(connection, "products", 3);

                populator.load("base");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 0);
                assertCount(connection, "invoice_details", 0);
                assertCount(connection, "products", 3);
            }
        }
    }
}
