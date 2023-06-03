package org.dandoy.dbpop;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.dandoy.dbpop.download.TableExecutionModel;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TableExecutionModelTest {
    @Test
    void name() throws IOException {
        URL url = getClass().getClassLoader().getResource("org/dandoy/dbpop/mssql/fullTableExecutionModel1.json");
        TableExecutionModel tableExecutionModel = new ObjectMapper().readValue(url, TableExecutionModel.class);
        assertNotNull(tableExecutionModel);
        assertEquals(2, tableExecutionModel.constraints().size());
        assertEquals("invoices_customers_fk", tableExecutionModel.constraints().get(0).constraintName());
        assertEquals("invoice_details_invoices_fk", tableExecutionModel.constraints().get(1).constraintName());
        assertEquals(1, tableExecutionModel.constraints().get(1).constraints().size());
        assertEquals("invoice_details_products_fk", tableExecutionModel.constraints().get(1).constraints().get(0).constraintName());
    }
}