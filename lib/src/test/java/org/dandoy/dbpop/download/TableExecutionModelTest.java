package org.dandoy.dbpop.download;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TableExecutionModelTest {
    @Test
    void name() throws IOException {
        URL url = getClass().getResource("fullTableExecutionModel1.json");
        TableExecutionModel tableExecutionModel = new ObjectMapper().readValue(url, TableExecutionModel.class);
        assertNotNull(tableExecutionModel);
        assertEquals(2, tableExecutionModel.getConstraints().size());
        assertEquals("invoices_customers_fk", tableExecutionModel.getConstraints().get(0).getConstraintName());
        assertEquals("invoice_details_invoices_fk", tableExecutionModel.getConstraints().get(1).getConstraintName());
        assertEquals(1, tableExecutionModel.getConstraints().get(1).getConstraints().size());
        assertEquals("invoice_details_products_fk", tableExecutionModel.getConstraints().get(1).getConstraints().get(0).getConstraintName());
    }
}