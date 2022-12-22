package org.dandoy.dbpop.download;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;

class TableExecutionModelTest {
    @Test
    void name() throws IOException {
        URL url = getClass().getResource("tableExecutionModel1.json");
        TableExecutionModel tableExecutionModel = new ObjectMapper().readValue(url, TableExecutionModel.class);
        Assertions.assertEquals("invoices", tableExecutionModel.getTableName().getTable());
    }
}