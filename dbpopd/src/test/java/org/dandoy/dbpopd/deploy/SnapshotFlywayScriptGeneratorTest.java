package org.dandoy.dbpopd.deploy;

import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.tests.mssql.DbPopContainerTest;
import org.dandoy.dbpopd.mssql.DbPopDatabaseSetup;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DbPopContainerTest(target = true, withTargetTables = true)
class SnapshotFlywayScriptGeneratorTest {
    private static Database database;
    private static File baseTestDirectory;

    @BeforeAll
    static void beforeAll() throws IOException {
        baseTestDirectory = new File("./src/test/resources/org/dandoy/dbpopd/deploy").getCanonicalFile();
        database = DbPopDatabaseSetup.getTargetDatabase();
    }

    private static void generateFlywayScripts(String usecase, Consumer<String> consumer) {
        File tempDir = Files.createTempDir();
        try {
            File usecaseDirectory = new File(baseTestDirectory, usecase);
            File snapshotFile = new File(usecaseDirectory, "snapshot.zip");
            File codeDirectory = new File(usecaseDirectory, "code");
            new SnapshotFlywayScriptGenerator(database, snapshotFile, codeDirectory, tempDir)
                    .generateFlywayScripts("test");
            String script = FileUtils.readFileToString(new File(tempDir, "V1__test.sql"), StandardCharsets.UTF_8);
            consumer.accept(script);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            FileUtils.deleteQuietly(tempDir);
        }
    }

    @Test
    void testCreateTable() {
        generateFlywayScripts("createTable", script -> {
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
            assertTrue(script.contains("CREATE TABLE [master].[dbo].[customers]"));
            assertTrue(script.contains("ALTER TABLE [master].[dbo].[customers] ADD CONSTRAINT"));
            assertTrue(script.contains("ADD CONSTRAINT [invoices_customers_fk]"));

        });
    }

    @Test
    void testDropTable() {
        generateFlywayScripts("dropTable", script -> {
            /* The following objects should be dropped, except that the customers PK is filtered out
ALTER TABLE [master].[dbo].[invoices] DROP CONSTRAINT [invoices_customers_fk]
GO
ALTER TABLE [master].[dbo].[customers] DROP CONSTRAINT [PK__customer__CD65CB8597E6784F]
GO
DROP TABLE [master].[dbo].[customers]
GO
             */
            assertTrue(script.contains("DROP TABLE [master].[dbo].[customers]"));
            assertTrue(script.contains("ALTER TABLE [master].[dbo].[invoices] DROP CONSTRAINT"));
            assertFalse(script.contains("ALTER TABLE [master].[dbo].[customers] DROP CONSTRAINT"));
        });
    }
}