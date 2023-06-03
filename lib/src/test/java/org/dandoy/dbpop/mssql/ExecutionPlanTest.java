package org.dandoy.dbpop.mssql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.download.ExecutionContext;
import org.dandoy.dbpop.download.ExecutionMode;
import org.dandoy.dbpop.download.ExecutionPlan;
import org.dandoy.dbpop.download.TableExecutionModel;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpop.tests.TestUtils;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import static org.dandoy.dbpop.mssql.MsSqlTestUtils.*;

@Testcontainers
class ExecutionPlanTest {
    @Container
    @SuppressWarnings({"rawtypes", "resource"})
    public static MSSQLServerContainer mssql = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:latest").acceptLicense();

    public static final File DATASETS_DIRECTORY = new File(TestUtils.TEMP_DIR, "datasets");
    private static Database sourceDatabase;

    @BeforeAll
    static void beforeAll() throws SQLException {
        ConnectionBuilder connectionBuilder = new UrlConnectionBuilder(
                mssql.getJdbcUrl(),
                mssql.getUsername(),
                mssql.getPassword()
        );

        sourceDatabase = Database.createDatabase(connectionBuilder);
        SqlExecutor.execute(sourceDatabase.getConnection(),
                "/mssql/createdb.sql",
                "/mssql/create.sql",
                "/mssql/insert_data.sql"
        );

        TestUtils.prepareTempConfigDir();
    }

    @AfterAll
    static void afterAll() {
        sourceDatabase.close();
    }

    @BeforeEach
    void setUp() {
        TestUtils.delete(new File(DATASETS_DIRECTORY, "download"));
    }

    @AfterEach
    void tearDown() {
        TestUtils.delete(new File(DATASETS_DIRECTORY, "download"));
    }

    @Test
    void testFullModel() throws IOException {
        TestUtils.delete(TestUtils.TEMP_DIR);
        TableExecutionModel tableExecutionModel = readTableExecutionModel("fullTableExecutionModel1.json");
        ExecutionContext executionContext = ExecutionPlan.execute(sourceDatabase, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.SAVE, null);
        Map<TableName, Integer> rowCounts = executionContext.getRowCounts();
        assertRowCounts(rowCounts, invoices, 4);
        assertRowCounts(rowCounts, invoiceDetails, 7);
        assertRowCounts(rowCounts, customers, 2);
        assertRowCounts(rowCounts, products, 3);
    }

    @Test
    void testRowCountLimit() throws IOException {
        TableExecutionModel tableExecutionModel = readTableExecutionModel("fullTableExecutionModel1.json");
        ExecutionContext executionContext = ExecutionPlan.execute(sourceDatabase, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.COUNT, 3);
        Map<TableName, Integer> rowCounts = executionContext.getRowCounts();
        assertRowCounts(rowCounts, invoices, 3);
        assertRowCounts(rowCounts, invoiceDetails, 0);
        assertRowCounts(rowCounts, customers, 0);
        assertRowCounts(rowCounts, products, 0);
    }

    @Test
    void testSmallModel() throws IOException {
        TableExecutionModel tableExecutionModel = readTableExecutionModel("smallTableExecutionModel1.json");
        ExecutionContext executionContext = ExecutionPlan.execute(sourceDatabase, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.SAVE, null);
        Map<TableName, Integer> rowCounts = executionContext.getRowCounts();
        assertRowCounts(rowCounts, invoices, 4);
        assertRowCounts(rowCounts, invoiceDetails, null);
        assertRowCounts(rowCounts, customers, 2);
        assertRowCounts(rowCounts, products, null);
        Assertions.assertTrue(new File(DATASETS_DIRECTORY, "download/dbpop/dbo/invoices.csv").exists());
    }

    @Test
    void testCountSmallModel() throws IOException {
        TableExecutionModel tableExecutionModel = readTableExecutionModel("smallTableExecutionModel1.json");
        ExecutionContext executionContext = ExecutionPlan.execute(sourceDatabase, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.COUNT, null);
        Map<TableName, Integer> rowCounts = executionContext.getRowCounts();
        assertRowCounts(rowCounts, invoices, 4);
        assertRowCounts(rowCounts, invoiceDetails, null);
        assertRowCounts(rowCounts, customers, 2);
        assertRowCounts(rowCounts, products, null);
        Assertions.assertFalse(new File(DATASETS_DIRECTORY, "download/dbpop/dbo/invoices.csv").exists());
    }

    private TableExecutionModel readTableExecutionModel(String name) throws IOException {
        URL url = getClass().getResource(name);
        return new ObjectMapper().readValue(url, TableExecutionModel.class);
    }

    private static void assertRowCounts(Map<TableName, Integer> rowCounts, TableName tableName, Integer expected) {
        Integer actual = rowCounts.get(tableName);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testBadConstraints() {
        testBadConstraints("""
                {"constraints": [
                    {"constraintName": "invoices_customers_fk"},
                    {"constraintName": "bad_constraint"},
                    {"constraintName": "invoice_details_invoices_fk"}
                ]}""");
        testBadConstraints("""
                {"constraints": [
                    {"constraintName": "invoices_customers_fk"},
                    {"constraintName": "invoice_details_invoices_fk",
                     "constraints": [
                       {"constraintName": "invoice_details_products_fk"},
                       {"constraintName": "bad_constraint"}
                    ]}
                ]}""");

    }

    static void testBadConstraints(@Language("JSON") String json) {
        try {
            TableExecutionModel tableExecutionModel = new ObjectMapper().readValue(json, TableExecutionModel.class);
            Assertions.assertThrows(RuntimeException.class, () ->
                    ExecutionPlan.execute(sourceDatabase, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.SAVE, null)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}