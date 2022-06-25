package org.dandoy.dbpop;

import org.dandoy.TestEnv;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.dandoy.test.MainTest.assertCount;

@EnabledIf("org.dandoy.TestEnv#hasDatabaseSetup")
public class DbPopTests {
    private static final Properties properties = TestEnv.readDbPop();
    private static final String jdbcurl = properties.getProperty("jdbcurl");
    private static final String username = properties.getProperty("username");
    private static final String password = properties.getProperty("password");
    private static final List<String> args = Arrays.asList(
            "--jdbcurl", jdbcurl,
            "--username", username,
            "--password", password,
            "--directory", "./src/test/resources/tests",
            "--verbose"
    );

    @Test
    void testDbPopMain() throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcurl, username, password)) {
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

    private void load(String... datasets) {
        List<String> args = new ArrayList<>(DbPopTests.args);
        args.addAll(Arrays.asList(datasets));
        DbPop.likeMain(args.toArray(new String[0]));
    }
}
