package org.dandoy.dbpopd;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlSetupServiceTest {
    @Test
    void testLinesToSql() {
        String scripts = "sql1;\n" +    // 0
                         "sql2\n" +     // 1
                         "go\n" +       // 2
                         "sql3\n" +     // 3
                         ";\n" +        // 4
                         "\n" +        // 5
                         "go\n" +       // 6
                         "go;\n" +      // 7
                         "sql4\n";      // 8

        List<SqlSetupService.Sql> sqls = SqlSetupService.linesToSql(List.of(scripts.split("\n")));
        assertEquals(4, sqls.size());

        assertEquals("sql1", sqls.get(0).sql());
        assertEquals(0, sqls.get(0).line());

        assertEquals("sql2", sqls.get(1).sql());
        assertEquals(1, sqls.get(1).line());

        assertEquals("sql3", sqls.get(2).sql());
        assertEquals(3, sqls.get(2).line());

        assertEquals("sql4", sqls.get(3).sql());
        assertEquals(8, sqls.get(3).line());
    }

    @Test
    void testLinesToSql2() {
        String scripts = """
                01   USE master;
                
                03   DROP TABLE IF EXISTS invoice_details;
                04   DROP TABLE IF EXISTS invoices;
                
                06   CREATE TABLE customers
                07   (
                08       customer_id INT PRIMARY KEY IDENTITY,
                09       name        VARCHAR(32)
                10   );
                
                12   CREATE TABLE products
                13   (
                14       product_id INT PRIMARY KEY IDENTITY,
                15       part_no    VARCHAR(32)  NOT NULL,
                16       part_desc  VARCHAR(255) NOT NULL
                17   );""";
        List<SqlSetupService.Sql> sqls = SqlSetupService.linesToSql(List.of(scripts.split("\n")));
        assertEquals(5, sqls.size());

        assertEquals(1, sqls.get(0).line());
        assertEquals(3, sqls.get(1).line());
        assertEquals(4, sqls.get(2).line());
        assertEquals(6, sqls.get(3).line());
        assertEquals(12, sqls.get(4).line());
    }
}