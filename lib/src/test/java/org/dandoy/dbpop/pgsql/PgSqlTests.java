package org.dandoy.dbpop.pgsql;

import org.dandoy.LocalCredentials;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * I need to rethink the tests since postgres makes it hard to connect across databases.
 */
@Disabled("Not supporting postgres at this time.")
@Testcontainers
public class PgSqlTests {
    @Container
    @SuppressWarnings({"rawtypes"})
    public static PostgreSQLContainer targetPgsql = new PostgreSQLContainer<>("postgres:latest");
    private static DatabaseProxy targetDatabase;
    private static Connection targetConnection;

    @BeforeAll
    static void beforeAll() throws SQLException {
        ConnectionBuilder connectionBuilder = new UrlConnectionBuilder(
                targetPgsql.getJdbcUrl(),
                targetPgsql.getUsername(),
                targetPgsql.getPassword()
        );
        targetDatabase = Database.createDatabase(connectionBuilder);
        targetConnection = targetDatabase.getConnection();
        SqlExecutor.execute(targetConnection,
                "/pgsql/drop.sql",
                "/pgsql/create.sql"
        );
    }

    @AfterAll
    static void afterAll() {
        targetDatabase.close();
    }

    @Test
    void name() {
        Table table = targetDatabase.getTable(PgSqlTestUtils.invoices);
        {
            assertNotNull(table);
            assertEquals(3, table.getColumns().size());
            assertEquals(
                    List.of("invoice_id", "customer_id", "invoice_date"),
                    table.getColumns().stream().map(Column::getName).toList()
            );
            assertEquals(1, table.getIndexes().size());
            assertNotNull(table.getIndexes().get(0).getName());
            assertEquals("invoice_id", table.getIndexes().get(0).getColumns().get(0));
            assertNotNull(table.getPrimaryKey());
            assertEquals(1, table.getForeignKeys().size());
            assertEquals("customer_id", table.getForeignKeys().get(0).getPkColumns().get(0));
            assertEquals("customers", table.getForeignKeys().get(0).getPkTableName().getTable());
            assertEquals("invoices", table.getForeignKeys().get(0).getFkTableName().getTable());
            assertNotNull(table.getForeignKeys().get(0).getName());
        }

        List<ForeignKey> relatedForeignKeys = targetDatabase.getRelatedForeignKeys(PgSqlTestUtils.customers);
        {
            assertEquals(1, relatedForeignKeys.size());
            ForeignKey foreignKey = relatedForeignKeys.get(0);
            assertEquals("invoices_customers_fk", foreignKey.getName());
            assertEquals("customers", foreignKey.getPkTableName().getTable());
            assertEquals("invoices", foreignKey.getFkTableName().getTable());
        }
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
