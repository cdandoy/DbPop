package org.dandoy.dbpop.download;

import org.dandoy.dbpop.database.TableName;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OutputFileTest {
    @Test
    void name() {
        OutputFile outputFile = OutputFile.createOutputFile(new File("src/test/resources/mssql"), "base", new TableName("master", "dbo", "customers"), false);
        assertEquals("customer_id", outputFile.getHeaders().get(0));
    }
}