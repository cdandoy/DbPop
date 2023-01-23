package org.dandoy.dbpop.download;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dandoy.LocalCredentials;
import org.dandoy.DbPopUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.tests.TestUtils;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;

import static org.dandoy.DbPopUtils.*;

@EnabledIf("org.dandoy.DbPopUtils#hasSourceMssql")
class ExecutionPlanTest {

    public static final File DATASETS_DIRECTORY = new File("src/test/resources/mssql");
    public static final LocalCredentials LOCAL_CREDENTIALS = LocalCredentials.from("mssql");
    private static Database sourceDatabase;


    @BeforeAll
    static void beforeAll() {
        DbPopUtils.prepareMssqlSource();
        sourceDatabase = Database.createDatabase(LOCAL_CREDENTIALS.sourceConnectionBuilder());
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
        TableExecutionModel tableExecutionModel = readTableExecutionModel("fullTableExecutionModel1.json");
        ExecutionContext executionContext = ExecutionPlan.execute(sourceDatabase, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.SAVE, null);
        Map<TableName, Integer> rowCounts = executionContext.getRowCounts();
        assertRowCounts(rowCounts, invoices, 4);
        assertRowCounts(rowCounts, invoiceDetails, 7);
        assertRowCounts(rowCounts, customers, 0);
        assertRowCounts(rowCounts, products, 0);
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
        assertRowCounts(rowCounts, customers, 0);
        assertRowCounts(rowCounts, products, null);
        Assertions.assertTrue(new File(DATASETS_DIRECTORY, "download/master/dbo/invoices.csv").exists());
    }

    @Test
    void testCountSmallModel() throws IOException {
        TableExecutionModel tableExecutionModel = readTableExecutionModel("smallTableExecutionModel1.json");
        ExecutionContext executionContext = ExecutionPlan.execute(sourceDatabase, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.COUNT, null);
        Map<TableName, Integer> rowCounts = executionContext.getRowCounts();
        assertRowCounts(rowCounts, invoices, 4);
        assertRowCounts(rowCounts, invoiceDetails, null);
        assertRowCounts(rowCounts, customers, 0);
        assertRowCounts(rowCounts, products, null);
        Assertions.assertFalse(new File(DATASETS_DIRECTORY, "download/master/dbo/invoices.csv").exists());
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