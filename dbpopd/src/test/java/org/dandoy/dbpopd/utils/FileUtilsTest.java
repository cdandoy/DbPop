package org.dandoy.dbpopd.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FileUtilsTest {
    @Test
    void name() {
        assertEquals("a_b_c_d", FileUtils.toFileName("a_b_c_d"));
        assertEquals("a_b_c_d", FileUtils.toFileName("a_b@c:d"));
    }
}