package org.dandoy.test;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.DbPopUtils;
import org.dandoy.LocalCredentials;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseProxy;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIf("org.dandoy.DbPopUtils#hasMssql")
@Slf4j
public class SqlServerTests {

    @BeforeEach
    void setUp() {
        DbPopUtils.prepareMssqlTarget();
    }

    @Test
    void datasetNotFound() {
        assertThrows(RuntimeException.class, () -> {
            LocalCredentials localCredentials = LocalCredentials.from("mssql");
            try (Database database = localCredentials.createTargetDatabase()) {
                Populator populator = Populator.createPopulator(database, new File("src/test/resources/tests/"));
                populator.load("test_1_1");
            }
        });
    }

    @Test
    void tableDoesNotExist() {
        RuntimeException runtimeException = assertThrows(RuntimeException.class, () -> {
            LocalCredentials localCredentials = LocalCredentials.from("mssql");
            try (Database database = localCredentials.createTargetDatabase()) {
                Populator ignored = Populator.createPopulator(database, new File("src/test/resources/test_bad_table"));
                System.out.println("I should not be here");
            }
        });
        assertTrue(runtimeException.getMessage().contains("dbpop.dbo.bad_table"));
        assertTrue(runtimeException.getMessage().contains("bad_table.csv"));
    }

    @Test
    void testExpressions() {
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (DatabaseProxy database = Database.createDatabase(localCredentials.targetConnectionBuilder())) {
            Populator populator = Populator.createPopulator(database, new File("src/test/resources/test_expressions"));
            populator.load("base");
        }
    }

    @Test
    void mainTest() throws SQLException {
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (DatabaseProxy database = Database.createDatabase(localCredentials.targetConnectionBuilder())) {
            Populator populator = Populator.createPopulator(database, new File("src/test/resources/mssql"));
            try (Connection targetConnection = localCredentials.createTargetConnection()) {

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

    /**
     * product is in the static dataset and should only be loaded once per populator.
     */
    @Test
    void testStatic() throws SQLException {
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (DatabaseProxy database = Database.createDatabase(localCredentials.targetConnectionBuilder())) {
            Populator populator = Populator.createPopulator(database, new File("src/test/resources/mssql"));
            try (Connection targetConnection = localCredentials.createTargetConnection()) {

                populator.load("base");
                assertCount(targetConnection, "customers", 3);
                assertCount(targetConnection, "invoices", 4);
                assertCount(targetConnection, "invoice_details", 7);
                assertCount(targetConnection, "products", 3);

                // Insert a product (static dataset) before to load. the populator is not supposed to notice
                try (PreparedStatement preparedStatement = targetConnection.prepareStatement("INSERT INTO dbpop.dbo.products (part_no, part_desc) VALUES ('XXX', 'XXX')")) {
                    preparedStatement.execute();
                }
                populator.load("base");
                assertCount(targetConnection, "products", 4);

                // A new populator will reset products
                populator = Populator.createPopulator(database, new File("src/test/resources/mssql"));
                populator.load("base");
                assertCount(targetConnection, "products", 3);
            }
        }
    }

    public static void assertCount(Connection connection, String table, int expected) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM dbpop.dbo." + table)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) throw new RuntimeException();
                int actual = resultSet.getInt(1);
                assertEquals(expected, actual, String.format("Invalid row count for table %s", table));
            }
        }
    }
}
