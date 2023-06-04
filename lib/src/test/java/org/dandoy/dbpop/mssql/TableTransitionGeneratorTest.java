package org.dandoy.dbpop.mssql;

import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.Transition;
import org.dandoy.dbpop.database.mssql.*;
import org.dandoy.dbpop.tests.mssql.DbPopContainerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DbPopContainerTest(source = true, target = false)
class TableTransitionGeneratorTest {
    private SqlServerDatabase database;

    @BeforeEach
    void setUp() {
        database = DbPopDatabaseSetup.getSourceDatabase();
    }

    @AfterEach
    void tearDown() {
        database.close();
    }

    @Test
    void testTable() {
        TableTransitionGenerator tableTransitionGenerator = new TableTransitionGenerator(database);
        Transition transition = tableTransitionGenerator.generateTransition(new ObjectIdentifier("TABLE", "master", "dbo", "customers"),
                """
                        CREATE TABLE [master].[dbo].[customers]
                        (
                            [customer_id] INT IDENTITY (1,1) NOT NULL,
                            [mod1] VARCHAR(32),
                            [mod2] VARCHAR(32),
                            [mod3] VARCHAR(32),
                            [del1] INT,
                            [del2] INT,
                            [del3] INT
                        )
                        """,
                """
                        CREATE TABLE [master].[dbo].[customers]
                        (
                            [customer_id] INT IDENTITY (1,1) NOT NULL,
                            [mod1] VARCHAR(64),
                            [mod2] VARCHAR(64) NOT NULL,
                            [mod3] VARCHAR(64),
                            [add_1] INT,
                            [add_2] INT NOT NULL,
                            [add_3] INT
                        )
                        """
        );
        assertEquals(List.of(
                "ALTER TABLE [master].[dbo].[customers] DROP COLUMN [del1], [del2], [del3]",
                "ALTER TABLE [master].[dbo].[customers] ALTER COLUMN [mod1] VARCHAR (64)",
                "ALTER TABLE [master].[dbo].[customers] ALTER COLUMN [mod2] VARCHAR (64) NOT NULL",
                "ALTER TABLE [master].[dbo].[customers] ALTER COLUMN [mod3] VARCHAR (64)",
                "ALTER TABLE [master].[dbo].[customers] ADD [add_1] INT, [add_2] INT NOT NULL, [add_3] INT"
        ), transition.getSqls());
    }

    @Test
    void testForeignKey() {
        ForeignKeyTransitionGenerator tableTransitionGenerator = new ForeignKeyTransitionGenerator(database);
        Transition transition = tableTransitionGenerator.generateTransition(
                new ObjectIdentifier(
                        "FOREIGN_KEY_CONSTRAINT",
                        "master", "dbo", "fkname",
                        new ObjectIdentifier("TABLE", "master", "dbo", "customers")
                ),
                "ALTER TABLE master.dbo.cda ADD CONSTRAINT fkname FOREIGN KEY (invoice_id) REFERENCES master.dbo.customers (customer_id)",
                "ALTER TABLE master.dbo.cda ADD CONSTRAINT fkname FOREIGN KEY (invoice_id) REFERENCES master.dbo.invoices (invoice_id)"
        );
        assertEquals(List.of(
                "ALTER TABLE [master].[dbo].[customers] DROP CONSTRAINT [fkname]",
                "ALTER TABLE master.dbo.cda ADD CONSTRAINT fkname FOREIGN KEY (invoice_id) REFERENCES master.dbo.invoices (invoice_id)"
        ), transition.getSqls());
    }

    @Test
    void testStoredProcedure() {
        ChangeCreateToAlterTransitionGenerator tableTransitionGenerator = new ChangeCreateToAlterTransitionGenerator(database);
        Transition transition = tableTransitionGenerator.generateTransition(
                new ObjectIdentifier("SQL_STORED_PROCEDURE", "master", "dbo", "GetInvoices"),
                """
                        CREATE PROCEDURE GetInvoices @invoiceId INT
                        AS
                        BEGIN
                            SELECT 1
                        END
                        """,
                """
                        CREATE PROCEDURE GetInvoices @invoiceId INT
                        AS
                        BEGIN
                            SELECT 2
                        END
                        """
        );
        assertEquals(List.of(
                "USE master",
                """
                        ALTER PROCEDURE GetInvoices @invoiceId INT
                        AS
                        BEGIN
                            SELECT 2
                        END
                        """

        ), transition.getSqls());
    }

    @Test
    void testIndex() {
        IndexTransitionGenerator tableTransitionGenerator = new IndexTransitionGenerator(database);
        Transition transition = tableTransitionGenerator.generateTransition(
                new ObjectIdentifier("INDEX", "master", "dbo", "invoices_invoice_date_index", new ObjectIdentifier("TABLE", "master", "dbo", "invoices")),
                "CREATE INDEX [invoices_invoice_date_index] ON [master].[dbo].[invoices] ([invoice_id], [invoice_date])",
                "CREATE INDEX [invoices_invoice_date_index] ON [master].[dbo].[invoices] ([invoice_date])"
        );
        assertEquals(List.of(
                "DROP INDEX [invoices_invoice_date_index] ON [master].[dbo].[invoices]",
                "CREATE INDEX [invoices_invoice_date_index] ON [master].[dbo].[invoices] ([invoice_date])"
        ), transition.getSqls());
    }
}