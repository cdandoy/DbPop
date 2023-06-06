package org.dandoy.dbpopd.deploy;

import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.tests.mssql.DbPopContainerTest;
import org.dandoy.dbpopd.mssql.DbPopDatabaseSetup;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DbPopContainerTest(target = true, withTargetTables = true)
class SnapshotSqlScriptGeneratorTest {
    private static Database database;
    private static File baseTestDirectory;

    @BeforeAll
    static void beforeAll() throws IOException {
        baseTestDirectory = new File("./src/test/resources/org/dandoy/dbpopd/deploy").getCanonicalFile();
        database = DbPopDatabaseSetup.getTargetDatabase();
    }

    private static void generateScripts(String usecase, BiConsumer<String, String> biConsumer) {
        File usecaseDirectory = new File(baseTestDirectory, usecase);
        File snapshotFile = new File(usecaseDirectory, "snapshot.zip");
        File codeDirectory = new File(usecaseDirectory, "code");
        SnapshotSqlScriptGenerator generator = new SnapshotSqlScriptGenerator(database, snapshotFile, codeDirectory);
        try {
            File deployFile = File.createTempFile("deploy", ".sql");
            try {
                File undeployFile = File.createTempFile("undeploy", ".sql");
                try {
                    generator.generateSqlScripts(deployFile, undeployFile);
                    String deploy = FileUtils.readFileToString(deployFile, StandardCharsets.UTF_8);
                    String undeploy = FileUtils.readFileToString(undeployFile, StandardCharsets.UTF_8);
                    biConsumer.accept(deploy, undeploy);
                } finally {
                    assertTrue(undeployFile.delete());
                }
            } finally {
                assertTrue(deployFile.delete());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testCreateTable() {
        generateScripts("createTable", (deploy, undeploy) -> {
            /* expected:
CREATE TABLE [master].[dbo].[customers]
(
    [customer_id] INT IDENTITY (1,1) NOT NULL,
    [name] VARCHAR(32)
)

GO
ALTER TABLE [master].[dbo].[customers] ADD CONSTRAINT [PK__customer__CD65CB8597E6784F] PRIMARY KEY ([customer_id])
GO
ALTER TABLE [master].[dbo].[invoices] ADD CONSTRAINT [invoices_customers_fk] FOREIGN KEY ([customer_id]) REFERENCES [master].[dbo].[customers] ([customer_id])
GO
             */
            assertTrue(deploy.contains("CREATE TABLE [master].[dbo].[customers]"));
            assertTrue(deploy.contains("ALTER TABLE [master].[dbo].[customers] ADD CONSTRAINT"));
            assertTrue(deploy.contains("ADD CONSTRAINT [invoices_customers_fk]"));

            /* The following objects should be dropped, except that the customers PK is filtered out
ALTER TABLE [master].[dbo].[invoices] DROP CONSTRAINT [invoices_customers_fk]
GO
ALTER TABLE [master].[dbo].[customers] DROP CONSTRAINT [PK__customer__CD65CB8597E6784F]
GO
DROP TABLE [master].[dbo].[customers]
GO
             */
            assertTrue(undeploy.contains("DROP TABLE [master].[dbo].[customers]"));
            assertTrue(undeploy.contains("ALTER TABLE [master].[dbo].[invoices] DROP CONSTRAINT"));
            assertFalse(undeploy.contains("ALTER TABLE [master].[dbo].[customers] DROP CONSTRAINT"));

        });
    }

    @Test
    void testDropTable() {
        generateScripts("dropTable", (deploy, undeploy) -> {
                  /* The following objects should be dropped, except that the customers PK is filtered out
ALTER TABLE [master].[dbo].[invoices] DROP CONSTRAINT [invoices_customers_fk]
GO
ALTER TABLE [master].[dbo].[customers] DROP CONSTRAINT [PK__customer__CD65CB8597E6784F]
GO
DROP TABLE [master].[dbo].[customers]
GO
             */
            assertTrue(deploy.contains("DROP TABLE [master].[dbo].[customers]"));
            assertTrue(deploy.contains("ALTER TABLE [master].[dbo].[invoices] DROP CONSTRAINT"));
            assertFalse(deploy.contains("ALTER TABLE [master].[dbo].[customers] DROP CONSTRAINT"));

            /* expected:
CREATE TABLE [master].[dbo].[customers]
(
    [customer_id] INT IDENTITY (1,1) NOT NULL,
    [name] VARCHAR(32)
)

GO
ALTER TABLE [master].[dbo].[customers] ADD CONSTRAINT [PK__customer__CD65CB8597E6784F] PRIMARY KEY ([customer_id])
GO
ALTER TABLE [master].[dbo].[invoices] ADD CONSTRAINT [invoices_customers_fk] FOREIGN KEY ([customer_id]) REFERENCES [master].[dbo].[customers] ([customer_id])
GO
             */
            assertTrue(undeploy.contains("CREATE TABLE [master].[dbo].[customers]"));
            assertTrue(undeploy.contains("ALTER TABLE [master].[dbo].[customers] ADD CONSTRAINT"));
            assertTrue(undeploy.contains("ADD CONSTRAINT [invoices_customers_fk]"));
        });
    }
}