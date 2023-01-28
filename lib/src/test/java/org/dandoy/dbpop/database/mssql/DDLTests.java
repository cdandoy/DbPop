package org.dandoy.dbpop.database.mssql;

import org.dandoy.LocalCredentials;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

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
                String ddl = table.toDDL(database);
                System.out.println(ddl);
            }
        }
    }

    @Test
    void fullTest1433() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;database=tempdb;trustServerCertificate=true", "sa", "SaltSpade2018")) {
            try (SqlServerDatabase database = new SqlServerDatabase(connection)) {
                for (Table table : database.getTables()) {
                    String ddl = table.toDDL(database);
                    System.out.println(ddl);
                }
            }
        }
    }

    @Test
    void fullTest2433() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost:2433;database=tempdb;trustServerCertificate=true", "sa", "GlobalTense1010")) {
            try (SqlServerDatabase database = new SqlServerDatabase(connection)) {
                for (Table table : database.getTables()) {
                    String ddl = table.toDDL(database);
                    System.out.println(ddl);
                }
            }
        }
    }
}
