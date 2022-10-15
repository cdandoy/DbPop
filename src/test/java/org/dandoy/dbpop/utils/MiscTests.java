package org.dandoy.dbpop.utils;

import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MiscTests {
    @Test
    void testBuilder() {
        Populator.Builder builder = Populator.builder()
                .setDbUrl("DbUrl")
                .setDbUser("DbUser")
                .setDbPassword("DbPassword");
        assertEquals("DbUrl", builder.getDbUrl());
        assertEquals("DbUser", builder.getDbUser());
        assertEquals("DbPassword", builder.getDbPassword());
    }
}
