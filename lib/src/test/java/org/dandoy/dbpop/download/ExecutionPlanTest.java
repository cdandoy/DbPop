package org.dandoy.dbpop.download;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dandoy.LocalCredentials;
import org.dandoy.TestUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.upload.Populator;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import static org.dandoy.TestUtils.*;

class ExecutionPlanTest {

    public static final File DATASETS_DIRECTORY = new File("src/test/resources/mssql");
    public static final LocalCredentials LOCAL_CREDENTIALS = LocalCredentials.from("mssql");
    private static Connection connection;
    private static Database database;

    @BeforeAll
    static void beforeAll() throws SQLException {
        connection = LOCAL_CREDENTIALS.createConnection();
        database = Database.createDatabase(connection);
    }

    @AfterAll
    static void afterAll() throws SQLException {
        database.close();
        connection.close();
    }

    @BeforeEach
    void setUp() {
        TestUtils.delete(new File(DATASETS_DIRECTORY, "download"));
        try (Populator populator = LOCAL_CREDENTIALS
                .populator()
                .setDirectory(DATASETS_DIRECTORY)
                .build()) {
            populator.load("invoices");
        }
    }

    @AfterEach
    void tearDown() {
        TestUtils.delete(new File(DATASETS_DIRECTORY, "download"));
    }

    @Test
    void testFullModel() throws IOException {
        URL url = getClass().getResource("fullTableExecutionModel1.json");
        TableExecutionModel tableExecutionModel = new ObjectMapper().readValue(url, TableExecutionModel.class);
        Map<TableName, Integer> rowCounts = ExecutionPlan.execute(database, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.SAVE, null);
        assertRowCounts(rowCounts, invoices, 4);
        assertRowCounts(rowCounts, invoiceDetails, 7);
        assertRowCounts(rowCounts, customers, 0);
        assertRowCounts(rowCounts, products, 0);
    }

    @Test
    void testRowCountLimit() throws IOException {
        URL url = getClass().getResource("fullTableExecutionModel1.json");
        TableExecutionModel tableExecutionModel = new ObjectMapper().readValue(url, TableExecutionModel.class);
        Map<TableName, Integer> rowCounts = ExecutionPlan.execute(database, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.SAVE, 3);
        assertRowCounts(rowCounts, invoices, 3);
        assertRowCounts(rowCounts, invoiceDetails, 0);
        assertRowCounts(rowCounts, customers, 0);
        assertRowCounts(rowCounts, products, 0);
    }

    @Test
    void testSmallModel() throws IOException {
        URL url = getClass().getResource("smallTableExecutionModel1.json");
        TableExecutionModel tableExecutionModel = new ObjectMapper().readValue(url, TableExecutionModel.class);
        Map<TableName, Integer> rowCounts = ExecutionPlan.execute(database, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.SAVE, null);
        assertRowCounts(rowCounts, invoices, 4);
        assertRowCounts(rowCounts, invoiceDetails, null);
        assertRowCounts(rowCounts, customers, 0);
        assertRowCounts(rowCounts, products, null);
        Assertions.assertTrue(new File(DATASETS_DIRECTORY, "download/master/dbo/invoices.csv").exists());
    }

    @Test
    void testCountSmallModel() throws IOException {
        URL url = getClass().getResource("smallTableExecutionModel1.json");
        TableExecutionModel tableExecutionModel = new ObjectMapper().readValue(url, TableExecutionModel.class);
        Map<TableName, Integer> rowCounts = ExecutionPlan.execute(database, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.COUNT, null);
        assertRowCounts(rowCounts, invoices, 4);
        assertRowCounts(rowCounts, invoiceDetails, null);
        assertRowCounts(rowCounts, customers, 0);
        assertRowCounts(rowCounts, products, null);
        Assertions.assertFalse(new File(DATASETS_DIRECTORY, "download/master/dbo/invoices.csv").exists());
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
                    ExecutionPlan.execute(database, DATASETS_DIRECTORY, "download", invoices, tableExecutionModel, Collections.emptyList(), Collections.emptySet(), ExecutionMode.SAVE, null)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}