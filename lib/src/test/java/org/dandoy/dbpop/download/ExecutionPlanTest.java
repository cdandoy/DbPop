package org.dandoy.dbpop.download;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dandoy.LocalCredentials;
import org.dandoy.TestUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.upload.Populator;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

import static org.dandoy.TestUtils.invoices;

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
    }

    @AfterEach
    void tearDown() {
        TestUtils.delete(new File(DATASETS_DIRECTORY, "download"));
    }

    @Test
    void name() throws IOException {
        try (Populator populator = LOCAL_CREDENTIALS
                .populator()
                .setDirectory(DATASETS_DIRECTORY)
                .build()) {
            populator.load("invoices");
        }

        URL url = getClass().getResource("fullTableExecutionModel1.json");
        TableExecutionModel tableExecutionModel = new ObjectMapper().readValue(url, TableExecutionModel.class);
        try (ExecutionPlan executionPlan = new ExecutionPlan(database, DATASETS_DIRECTORY, "download")) {
            executionPlan.build(invoices, tableExecutionModel, Collections.emptyList());
            executionPlan.download(Collections.emptySet());
        }
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
            try (ExecutionPlan executionPlan = new ExecutionPlan(database, DATASETS_DIRECTORY, "download")) {
                Assertions.assertThrows(RuntimeException.class, () -> executionPlan.build(invoices, tableExecutionModel, Collections.emptyList()));
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}