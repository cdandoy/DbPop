package org.dandoy.dbpop.mssql;

import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpop.upload.Populator;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.dandoy.dbpop.mssql.MsSqlTestUtils.customers;
import static org.dandoy.dbpop.mssql.MsSqlTestUtils.invoices;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class SqlServerTests {

    @Container
    @SuppressWarnings({"rawtypes", "resource"})
    public MSSQLServerContainer targetMssql = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:latest").acceptLicense();
    private DatabaseProxy targetDatabase;
    private Connection targetConnection;

    @BeforeEach
    void setUp() throws SQLException {
        ConnectionBuilder connectionBuilder = new UrlConnectionBuilder(
                targetMssql.getJdbcUrl(),
                targetMssql.getUsername(),
                targetMssql.getPassword()
        );
        targetDatabase = Database.createDatabase(connectionBuilder);
        targetConnection = targetDatabase.getConnection();
        SqlExecutor.execute(targetConnection,
                "/mssql/createdb.sql",
                "/mssql/create.sql",
                "/mssql/insert_data.sql"
        );
    }

    @AfterEach
    void tearDown() {
        targetDatabase.close();
    }

    @Test
    void testDbPopMain() throws SQLException {
        Populator populator = Populator.createPopulator(targetDatabase, new File("src/test/resources/mssql"));
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

        ////////////////////////////////////////////////////////////////////////////////
        // Test Identity

        // Insert a new customer
        long customerId;
        try (PreparedStatement preparedStatement = targetConnection.prepareStatement("INSERT INTO dbpop.dbo.invoices(customer_id, invoice_date) OUTPUT inserted.invoice_id VALUES (101, GETDATE())")) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertTrue(resultSet.next());
                customerId = resultSet.getLong(1);
            }
        }

        // Reset the data
        populator.load("base");

        // Insert a new customer again, the identity column should have been reset, we should have the same customer_id
        try (PreparedStatement preparedStatement = targetConnection.prepareStatement("INSERT INTO dbpop.dbo.invoices(customer_id, invoice_date) OUTPUT inserted.invoice_id VALUES (101, GETDATE())")) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertTrue(resultSet.next());
                assertEquals(customerId, resultSet.getLong(1));
            }
        }

        ////////////////////////////////////////////////////////////////////////////////
        // Test Static
        // Insert a product (static dataset) before to load. the populator is not supposed to notice
        try (PreparedStatement preparedStatement = targetConnection.prepareStatement("INSERT INTO dbpop.dbo.products (part_no, part_desc) VALUES ('XXX', 'XXX')")) {
            preparedStatement.execute();
        }
        populator.load("base");
        assertCount(targetConnection, "products", 4);

        // A new populator will reset products
        populator = Populator.createPopulator(targetDatabase, new File("src/test/resources/mssql"));
        populator.load("base");
        assertCount(targetConnection, "products", 3);
    }

    @Test
    void datasetNotFound() {
        assertThrows(RuntimeException.class, () -> {
            Populator populator = Populator.createPopulator(targetDatabase, new File("src/test/resources/tests/"));
            populator.load("test_1_1");
        });
    }

    @Test
    void tableDoesNotExist() {
        RuntimeException runtimeException = assertThrows(RuntimeException.class, () -> {
            Populator ignored = Populator.createPopulator(targetDatabase, new File("src/test/resources/test_bad_table"));
            System.out.println("I should not be here");
        });
        assertTrue(runtimeException.getMessage().contains("dbpop.dbo.bad_table"));
        assertTrue(runtimeException.getMessage().contains("bad_table.csv"));
    }

    @Test
    void testExpressions() {
        Populator populator = Populator.createPopulator(targetDatabase, new File("src/test/resources/test_expressions"));
        populator.load("base");
    }

    @Test
    void testDatabaseFunctionality() {
        Table table = targetDatabase.getTable(invoices);
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

        List<ForeignKey> relatedForeignKeys = targetDatabase.getRelatedForeignKeys(customers);
        {
            assertEquals(2, relatedForeignKeys.size());
            ForeignKey foreignKey = relatedForeignKeys.stream()
                    .filter(it -> "invoices_customers_fk".equals(it.getName()))
                    .findFirst().orElseThrow();
            assertEquals("invoices_customers_fk", foreignKey.getName());
            assertEquals("customers", foreignKey.getPkTableName().getTable());
            assertEquals("invoices", foreignKey.getFkTableName().getTable());
        }

    }

    /**
     * Verify that DatabaseIntrospector.visitModuleMetas() returns the same objects as the two versions of DatabaseIntrospector.visitModuleDefinitions()
     */
    @Test
    void introspectorTest() {
        List<ObjectIdentifier> metaIdentifiers = new ArrayList<>();
        targetDatabase.createDatabaseIntrospector()
                .visitModuleMetas("master", new DatabaseVisitor() {
                    @Override
                    public void moduleMeta(ObjectIdentifier objectIdentifier, Date modifyDate) {
                        metaIdentifiers.add(objectIdentifier);
                    }
                });

        List<ObjectIdentifier> defIdentifiers = new ArrayList<>();
        targetDatabase.createDatabaseIntrospector()
                .visitModuleDefinitions("master", new DatabaseVisitor() {
                    @Override
                    public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String definition) {
                        defIdentifiers.add(objectIdentifier);
                    }
                });

        List<ObjectIdentifier> defIdentifiers2 = new ArrayList<>();
        targetDatabase.createDatabaseIntrospector()
                .visitModuleDefinitions(metaIdentifiers, new DatabaseVisitor() {
                    @Override
                    public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String definition) {
                        defIdentifiers2.add(objectIdentifier);
                    }
                });

        Collections.sort(metaIdentifiers);
        Collections.sort(defIdentifiers);
        assertEquals(metaIdentifiers, defIdentifiers);

        Collections.sort(defIdentifiers2);
        assertEquals(defIdentifiers, defIdentifiers2);
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
