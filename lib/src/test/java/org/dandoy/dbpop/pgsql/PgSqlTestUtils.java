package org.dandoy.dbpop.pgsql;

import org.dandoy.dbpop.database.TableName;

public class PgSqlTestUtils {
    static final TableName invoices = new TableName("dbpop", "public", "invoices");
    static final TableName customers = new TableName("dbpop", "public", "customers");
}
