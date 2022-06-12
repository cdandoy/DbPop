package org.dandoy.test;

import org.dandoy.dbpop.Populator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestUsage {
    private static Populator populator;

    @BeforeAll
    static void beforeAll() {
        populator = Populator.build();
    }

    @AfterAll
    static void afterAll() {
        populator.close();
    }

    @Test
    void myTest() {
        populator.load("base");
    }
}
