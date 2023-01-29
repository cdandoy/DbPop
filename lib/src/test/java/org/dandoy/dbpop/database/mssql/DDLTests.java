package org.dandoy.dbpop.database.mssql;

import org.dandoy.LocalCredentials;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.utils.ElapsedStopWatch;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.*;

@Disabled
public class DDLTests {
    @Test
    void datatypeTest() throws SQLException {
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Connection connection = localCredentials.createTargetConnection()) {
            connection.setAutoCommit(true);
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS master.dbo.ddl_test");
                statement.execute("""
                        CREATE TABLE master.dbo.ddl_test
                        (
                            id             INT IDENTITY (1,1) NOT NULL,
                            dt_varchar_100 VARCHAR(100),
                            dt_varchar_max VARCHAR(MAX),
                            dt_datetime    DATETIME,
                            dt_datetime2   DATETIME2,
                            def_int        INT         DEFAULT (0),
                            def_varchar    VARCHAR(32) DEFAULT ('Default')
                        )
                        """);
            }

            try (SqlServerDatabase database = new SqlServerDatabase(connection)) {
                Table table = database.getTable(new TableName("master", "dbo", "ddl_test"));
                String ddl = table.tableDDL(database);
                System.out.println(ddl);
            }
        }
    }

    @Test
    void fullTest1433() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;database=tempdb;trustServerCertificate=true", "sa", "SaltSpade2018")) {
            try (SqlServerDatabase database = new SqlServerDatabase(connection)) {
                System.out.println("-----------------------------------------------------------");
                System.out.println("-- Tables");
                for (Table table : database.getTables()) {
                    System.out.println(table.tableDDL(database));
                }
                System.out.println("-----------------------------------------------------------");
                System.out.println("-- Indexes");
                for (Table table : database.getTables()) {
                    table.indexesDDLs(database).forEach(System.out::println);
                }
                System.out.println("-----------------------------------------------------------");
                System.out.println("-- Foreign Keys");
                for (Table table : database.getTables()) {
                    table.foreignKeyDDLs(database).forEach(System.out::println);
                }
            }
        }
    }

    @Test
    void fullTest2433() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost:2433;database=tempdb;trustServerCertificate=true", "sa", "GlobalTense1010")) {
            try (SqlServerDatabase database = new SqlServerDatabase(connection)) {
                for (Table table : database.getTables()) {
                    String ddl = table.tableDDL(database);
                    System.out.println(ddl);
                }
            }
        }
    }

    @Test
    void winDev() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlserver://10.131.3.228;database=tempdb;trustServerCertificate=true", "DBA", "secured@00")) {
            try (SqlServerDatabase database = new SqlServerDatabase(connection)) {
                SqlServerDatabaseIntrospector databaseIntrospector = database.createDatabaseIntrospector();
                DatabaseVisitor databaseVisitor = new DatabaseVisitor() {
                    @Override
                    public void catalog(String catalog) {
                        System.out.println("-------------------- " + catalog);
                        ElapsedStopWatch stopWatch = new ElapsedStopWatch();

                        databaseIntrospector.visitModuleMetas(this, catalog);
                        System.out.println("visitModuleMetas: " + stopWatch);

                        databaseIntrospector.visitModuleDefinitions(this, catalog);
                        System.out.println("visitModuleDefinitions: " + stopWatch);

                        databaseIntrospector.visitDependencies(this, catalog);
                        System.out.println("visitDependencies: " + stopWatch);
                    }
                };
                databaseIntrospector.visit(databaseVisitor);
            }
        }
    }
}
