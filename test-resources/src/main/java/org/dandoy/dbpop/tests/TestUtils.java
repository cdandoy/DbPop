package org.dandoy.dbpop.tests;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class TestUtils {
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

    public static void prepareTempDatasetDir() {
        try {
            File srcDir = new File("../files/config/datasets");
            File destDir = new File("../files/temp/datasets/");
            delete(destDir);
            FileUtils.copyDirectory(srcDir, destDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
