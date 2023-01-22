package org.dandoy;

import org.dandoy.dbpop.database.TableName;

public class DbPopUtils {
    public static final TableName invoices = new TableName("master", "dbo", "invoices");
    public static final TableName invoiceDetails = new TableName("master", "dbo", "invoice_details");
    public static final TableName customers = new TableName("master", "dbo", "customers");
    public static final TableName products = new TableName("master", "dbo", "products");

    public static void prepareMssqlSource() {
        LocalCredentials
                .from("mssql")
                .executeSource(
                        "/mssql/drop_tables.sql",
                        "/mssql/create_tables.sql",
                        "/mssql/insert_data.sql"
                );
    }

    public static void prepareMssqlTarget() {
        LocalCredentials
                .from("mssql")
                .executeTarget(
                        "/mssql/drop_tables.sql",
                        "/mssql/create_tables.sql"
                );
    }

    @SuppressWarnings("unused")
    public static void preparePgsqlSource() {
        LocalCredentials
                .from("pgsql")
                .executeSource(
                        "/pgsql/drop_tables.sql",
                        "/pgsql/create_tables.sql",
                        "/pgsql/insert_data.sql"
                );
    }

    public static void preparePgsqlTarget() {
        LocalCredentials
                .from("pgsql")
                .executeSource(
                        "/pgsql/drop_tables.sql",
                        "/pgsql/create_tables.sql"
                );
    }
}
