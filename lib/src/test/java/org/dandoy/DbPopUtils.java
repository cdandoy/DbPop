package org.dandoy;

import org.dandoy.dbpop.database.TableName;

public class DbPopUtils {
    public static final TableName invoices = new TableName("dbpop", "dbo", "invoices");
    public static final TableName invoiceDetails = new TableName("dbpop", "dbo", "invoice_details");
    public static final TableName customers = new TableName("dbpop", "dbo", "customers");
    public static final TableName products = new TableName("dbpop", "dbo", "products");

    public static boolean hasMssql() {
        return hasSourceMssql() && hasTargetMssql();
    }

    public static boolean hasSourceMssql() {
        return LocalCredentials.from("mssql").sourceConnectionBuilder() != null;
    }

    public static boolean hasTargetMssql() {
        return LocalCredentials.from("mssql").targetConnectionBuilder() != null;
    }

    public static boolean hasPgsql() {
        return hasSourcePgsql() && hasTargetPgsql();
    }

    public static boolean hasSourcePgsql() {
        return LocalCredentials.from("pgsql").sourceConnectionBuilder() != null;
    }

    public static boolean hasTargetPgsql() {
        return LocalCredentials.from("pgsql").targetConnectionBuilder() != null;
    }

    public static void prepareMssqlSource() {
        LocalCredentials
                .from("mssql")
                .createSourceDbPopDatabase()
                .executeSource(
                        "/mssql/drop.sql",
                        "/mssql/create.sql",
                        "/mssql/insert_data.sql"
                );
    }

    public static void prepareMssqlTarget() {
        LocalCredentials
                .from("mssql")
                .createTargetDbPopDatabase()
                .executeTarget(
                        "/mssql/drop.sql",
                        "/mssql/create.sql"
                );
    }

    @SuppressWarnings("unused")
    public static void preparePgsqlSource() {
        LocalCredentials
                .from("pgsql")
                .executeSource(
                        "/pgsql/drop.sql",
                        "/pgsql/create.sql",
                        "/pgsql/insert_data.sql"
                );
    }

    public static void preparePgsqlTarget() {
        LocalCredentials
                .from("pgsql")
                .executeSource(
                        "/pgsql/drop.sql",
                        "/pgsql/create.sql"
                );
    }
}
