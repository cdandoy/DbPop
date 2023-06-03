package org.dandoy.dbpop.mssql;

import org.dandoy.dbpop.database.TableName;

public class MsSqlTestUtils {
    public static final TableName invoices = new TableName("dbpop", "dbo", "invoices");
    public static final TableName invoiceDetails = new TableName("dbpop", "dbo", "invoice_details");
    public static final TableName customers = new TableName("dbpop", "dbo", "customers");
    public static final TableName products = new TableName("dbpop", "dbo", "products");
}
