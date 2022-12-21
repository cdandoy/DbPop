package org.dandoy.dbpop.database;

import org.dandoy.LocalCredentials;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PgSqlTests {
    @Test
    void name() throws SQLException {
        LocalCredentials localCredentials = LocalCredentials.from("pgsql");
        try (Connection connection = localCredentials.createConnection()) {
            try (Database database = Database.createDatabase(connection)) {
                Table table = database.getTable(new TableName("dbpop", "public", "invoices"));
                assertNotNull(table);
                assertEquals(3, table.columns().size());
                assertEquals(
                        List.of("invoice_id", "customer_id", "invoice_date"),
                        table.columns().stream().map(Column::getName).toList()
                );
                assertEquals(1, table.indexes().size());
                assertEquals("invoices_pkey", table.indexes().get(0).getName());
                assertEquals("invoice_id", table.indexes().get(0).getColumns().get(0));
                assertNotNull(table.primaryKey());
                assertEquals(1, table.foreignKeys().size());
                assertEquals("invoices_customer_id_fkey", table.foreignKeys().get(0).getName());
            }
        }
    }
}
