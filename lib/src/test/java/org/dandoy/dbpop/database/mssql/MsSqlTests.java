package org.dandoy.dbpop.database.mssql;

import org.dandoy.LocalCredentials;
import org.dandoy.DbPopUtils;
import org.dandoy.dbpop.database.Column;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ForeignKey;
import org.dandoy.dbpop.database.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.List;

import static org.dandoy.DbPopUtils.customers;
import static org.dandoy.DbPopUtils.invoices;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@EnabledIf("org.dandoy.DbPopUtils#hasTargetMssql")
public class MsSqlTests {
    @BeforeEach
    void setUp() {
        DbPopUtils.prepareMssqlTarget();
    }

    @Test
    void name() {
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Database targetDatabase = Database.createDatabase(localCredentials.targetConnectionBuilder())) {
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
                assertEquals(1, relatedForeignKeys.size());
                ForeignKey foreignKey = relatedForeignKeys.get(0);
                assertEquals("invoices_customers_fk", foreignKey.getName());
                assertEquals("customers", foreignKey.getPkTableName().getTable());
                assertEquals("invoices", foreignKey.getFkTableName().getTable());
            }
        }
    }
}
