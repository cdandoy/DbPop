package org.dandoy.test;

import org.dandoy.DbPopUtils;
import org.dandoy.LocalCredentials;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EnabledIf("org.dandoy.DbPopUtils#hasPgsql")
public class PostgresTests {
    private static Connection targetConnection;

    @BeforeAll
    public static void prepare() throws SQLException {
        targetConnection = LocalCredentials.from("mssql").createTargetConnection();
        DbPopUtils.preparePgsqlTarget();
    }

    @AfterAll
    static void afterAll() throws SQLException {
        targetConnection.close();
    }

    @Test
    void mainTest() throws SQLException {
        LocalCredentials localCredentials = LocalCredentials.from("pgsql");
        try (Database database = Database.createDatabase(localCredentials.targetConnectionBuilder())) {
            Populator populator = Populator.createPopulator(database, new File("src/test/resources/pgsql"));

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

    public static void assertCount(Connection connection, String table, int expected) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM dbpop.public." + table)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) throw new RuntimeException();
                int actual = resultSet.getInt(1);
                assertEquals(expected, actual, String.format("Invalid row count for table %s", table));
            }
        }
    }
}
