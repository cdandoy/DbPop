package org.dandoy.dbpop;

import org.dandoy.TestUtils;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.dandoy.test.SqlServerTests.assertCount;

@EnabledIf("org.dandoy.TestUtils#hasSqlServer")
public class DbPopTests {
    private static final List<String> args = Arrays.asList(
            "populate",
            "--path", "mssql"
    );

    @BeforeAll
    static void beforeAll() {
        TestUtils.executeSqlScript("mssql", "/mssql/test.sql");
    }

    @Test
    void testDbPopMain() throws SQLException {
        try (Populator populator = Populator.builder()
                .setEnvironment("mssql")
                .setPath("mssql")
                .build()) {
            try (Connection connection = populator.createConnection()) {
                load("base");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 0);
                assertCount(connection, "invoice_details", 0);
                assertCount(connection, "products", 3);

                load("base", "invoices");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 4);
                assertCount(connection, "invoice_details", 7);
                assertCount(connection, "products", 3);

                load("base");
                assertCount(connection, "customers", 3);
                assertCount(connection, "invoices", 0);
                assertCount(connection, "invoice_details", 0);
                assertCount(connection, "products", 3);
            }
        }
    }

    private void load(String... datasets) {
        List<String> args = new ArrayList<>(DbPopTests.args);
        args.addAll(Arrays.asList(datasets));
        DbPop.likeMain(args.toArray(new String[0]));
    }
}
