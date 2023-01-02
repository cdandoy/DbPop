package org.dandoy.dbpop.database;

import org.dandoy.LocalCredentials;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@EnabledIf("org.dandoy.TestUtils#hasTargetPgsql")
public class PgSqlTests {
    private static final TableName invoices = new TableName("dbpop", "public", "invoices");
    private static final TableName customers = new TableName("dbpop", "public", "customers");
    private static Connection targetConnection;
    private static Database targetDatabase;

    @BeforeAll
    static void beforeAll() throws SQLException {
        LocalCredentials localCredentials = LocalCredentials.from("pgsql");
        targetConnection = localCredentials.createTargetConnection();
        targetDatabase = Database.createDatabase(targetConnection);
    }

    @AfterAll
    static void afterAll() throws SQLException {
        targetDatabase.close();
        targetConnection.close();
    }

    @Test
    void name()  {
        Table table = targetDatabase.getTable(invoices);
        {
            assertNotNull(table);
            assertEquals(3, table.columns().size());
            assertEquals(
                    List.of("invoice_id", "customer_id", "invoice_date"),
                    table.columns().stream().map(Column::getName).toList()
            );
            assertEquals(1, table.indexes().size());
            assertNotNull(table.indexes().get(0).getName());
            assertEquals("invoice_id", table.indexes().get(0).getColumns().get(0));
            assertNotNull(table.primaryKey());
            assertEquals(1, table.foreignKeys().size());
            assertEquals("customer_id", table.foreignKeys().get(0).getPkColumns().get(0));
            assertEquals("customers", table.foreignKeys().get(0).getPkTableName().getTable());
            assertEquals("invoices", table.foreignKeys().get(0).getFkTableName().getTable());
            assertNotNull(table.foreignKeys().get(0).getName());
        }

        List<ForeignKey> relatedForeignKeys = targetDatabase.getRelatedForeignKeys(customers);
        {
            assertEquals(1, relatedForeignKeys.size());
            ForeignKey foreignKey = relatedForeignKeys.get(0);
            assertEquals("invoices_customers_fk", foreignKey.getName());
            assertEquals("customers", foreignKey.getPkTableName().getTable());
            assertEquals("invoices", foreignKey.getFkTableName().getTable());
        }
    }

    @Test
    void testSearch() {
        Set<TableName> tableNames = targetDatabase.searchTable("usto");
        assertEquals(1, tableNames.size());
        TableName tableName = tableNames.iterator().next();
        assertEquals(customers, tableName);
    }
}
