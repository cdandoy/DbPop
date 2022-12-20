package org.dandoy.dbpop.download2;

import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.download.OutputFile;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class OutputFileTest {
    @Test
    void name() {
        OutputFile outputFile = OutputFile.createOutputFile(new File("src/test/resources/mssql"), "base", new TableName("master", "dbo", "customers"));
        assertEquals("customer_id", outputFile.getHeaders().get(0));
    }
}