package org.dandoy.dbpop.fs;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;

class ClasspathCollectorTest {
    @Test
    void name() {
        List<File> files = new ClasspathCollector().collectClasspathFiles(ClasspathCollectorTest.class);
        assertFalse(files.isEmpty());
    }
}