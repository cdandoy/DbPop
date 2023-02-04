package org.dandoy.dbpop.tests;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class TestUtils {
    public static final File SRC_DIR = new File("../files/config/");
    public static final File TEMP_DIR = new File("../files/temp/");

    public static void delete(File file) {
        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                delete(f);
            }
        }
        if (!file.delete() && file.exists()) {
            throw new RuntimeException("Failed to delete " + file);
        }
    }

    public static void prepareTempConfigDir() {
        try {
            delete(TEMP_DIR);
            FileUtils.copyDirectory(SRC_DIR, TEMP_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteTempConfigDir() {
        delete(TEMP_DIR);
    }
}
