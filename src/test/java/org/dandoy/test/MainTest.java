package org.dandoy.test;

import org.dandoy.dbpop.Populator;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

public class MainTest {
    @Test
    void noCsvFiles() {
        assertThrows(RuntimeException.class, () -> {
            try (Populator ignored = Populator.builder()
                    .setDirectory("src/test/resources/test_no_datafiles")
                    .build()) {
                System.out.println("I should not be here");
            }
        });
    }

    @Test
    void datasetNotFound() {
        assertThrows(RuntimeException.class, () -> {
            try (Populator populator = Populator.builder()
                    .setDirectory("src/test/resources/tests")
                    .build()) {
                populator.load("test_1_1");
            }
        });
    }

    @Test
    void tableDoesNotExist() {
        RuntimeException runtimeException = assertThrows(RuntimeException.class, () -> {
            try (Populator ignored = Populator.builder()
                    .setDirectory("src/test/resources/test_bad_table")
                    .build()) {
                System.out.println("I should not be here");
            }
        });
        assertTrue(runtimeException.getMessage().contains("master.dbo.bad_table"));
        assertTrue(runtimeException.getMessage().contains("bad_table.csv"));
    }

    @Test
    void mainTest() throws SQLException {
        try (Populator populator = Populator.builder()
                .setDirectory("src/test/resources/tests")
                .build()) {
            try (Connection connection = populator.createConnection()) {

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

    public static void assertCount(Connection connection, String table, int expected) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM master.dbo." + table)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) throw new RuntimeException();
                int actual = resultSet.getInt(1);
                assertEquals(expected, actual, String.format("Invalid row count for table %s", table));
            }
        }
    }
}
