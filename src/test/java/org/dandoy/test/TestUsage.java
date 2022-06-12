package org.dandoy.test;

import org.dandoy.dbpop.Populator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestUsage {
    private static Populator populator;

    @BeforeAll
    static void beforeAll() {
        try {
            populator = Populator.build();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    static void afterAll() {
        try {
            populator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    void myTest() {
        long t0 = System.currentTimeMillis();
        int rows = populator.load("base");
        long t1 = System.currentTimeMillis();
        System.out.printf("%d rows loaded in %dms%n", rows, t1 - t0);
    }
}
