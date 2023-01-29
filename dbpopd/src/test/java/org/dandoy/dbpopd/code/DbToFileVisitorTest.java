package org.dandoy.dbpopd.code;

import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.database.mssql.SqlServerDatabase;
import org.junit.jupiter.api.Test;

import java.io.File;

class DbToFileVisitorTest {
    @Test
    void name() {
        UrlConnectionBuilder connectionBuilder = new UrlConnectionBuilder("jdbc:sqlserver://10.131.3.228;database=tempdb;trustServerCertificate=true", "DBA", "secured@00");
        try (SqlServerDatabase database = new SqlServerDatabase(connectionBuilder)) {
            DatabaseIntrospector databaseIntrospector = database.createDatabaseIntrospector();
            try (DbToFileVisitor visitor = new DbToFileVisitor(databaseIntrospector, new File("D:\\dbpop\\ops-win-dev\\config\\code"))) {
                databaseIntrospector.visit(visitor);
            }
        }
    }
}