package org.dandoy.dbpop.download;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.dandoy.TestUtils.customers;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OutputFileTest {
    @Test
    void name() {
        OutputFile outputFile = OutputFile.createOutputFile(new File("src/test/resources/mssql"), "base", customers, false);
        assertEquals("customer_id", outputFile.getHeaders().get(0));
    }
}