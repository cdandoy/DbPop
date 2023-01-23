package org.dandoy.dbpop.database;

import org.dandoy.LocalCredentials;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@EnabledIf("org.dandoy.DbPopUtils#hasTargetPgsql")
public class PgSqlTests {
    private static final TableName invoices = new TableName("dbpop", "public", "invoices");
    private static final TableName customers = new TableName("dbpop", "public", "customers");
    private static Database targetDatabase;

    @BeforeAll
    static void beforeAll() {
        LocalCredentials localCredentials = LocalCredentials.from("pgsql");
        targetDatabase = Database.createDatabase(localCredentials.targetConnectionBuilder());
    }

    @AfterAll
    static void afterAll() {
        targetDatabase.close();
    }

    @Test
    void name() {
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
}
