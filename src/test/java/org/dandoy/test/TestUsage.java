package org.dandoy.test;

import org.dandoy.dbpop.Populator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestUsage {
    private static Populator populator;

    @BeforeAll
    static void beforeAll()  {
        try {
            Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost;database=tempdb;trustServerCertificate=true", "sa", "");
            populator = Populator.builder()
                    .setDirectory(new File("./src/test/resources/datasets"))
                    .setConnection(connection)
                    .setVerbose(true)
                    .build();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    static void afterAll() {
        populator.close();
    }

    @Test
    void myTest() {
        int rows = populator.load("base");
        System.out.printf("%d rows loaded%n", rows);
    }
}
