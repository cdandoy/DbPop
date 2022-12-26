package org.dandoy.dbpop.database;

import org.dandoy.LocalCredentials;
import org.dandoy.dbpop.database.TableDependencies.TableDependency;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.dandoy.TestUtils.invoiceDetails;
import static org.dandoy.TestUtils.invoices;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TableDependenciesTest {
    public static final LocalCredentials LOCAL_CREDENTIALS = LocalCredentials.from("mssql");

    private static Connection connection;
    private static Database database;

    @BeforeAll
    static void beforeAll() throws SQLException {
        connection = LOCAL_CREDENTIALS.createConnection();
        database = Database.createDatabase(connection);
    }

    @AfterAll
    static void afterAll() throws SQLException {
        database.close();
        connection.close();
    }

    @Test
    void testinvoices()  {
        TableDependencies tableDependencies = new TableDependencies(database);
        tableDependencies.findDependencies(invoices);
        List<TableDependency> dependencies = tableDependencies.getDependencies();
        assertEquals(2, dependencies.size());
        assertNotNull(findByName(dependencies, "customers"));
        assertNotNull(findByName(dependencies, "invoice_details"));
    }

    @Test
    void testinvoicesAndDetails()  {
        TableDependencies tableDependencies = new TableDependencies(database);
        tableDependencies.findDependencies(invoices);
        tableDependencies.findDependencies(invoiceDetails);
        List<TableDependency> dependencies = tableDependencies.getDependencies();
        assertEquals(4, dependencies.size());
        assertNotNull(findByName(dependencies, "customers"));
        assertNotNull(findByName(dependencies, "products"));
        assertNotNull(findByName(dependencies, "invoice_details"));
        assertNotNull(findByName(dependencies, "invoices"));
    }

    private static TableDependency findByName(List<TableDependency> tableDependencies, String name) {
        return tableDependencies.stream().filter(it -> name.equals(it.tableName().getTable())).findFirst().orElse(null);
    }
}