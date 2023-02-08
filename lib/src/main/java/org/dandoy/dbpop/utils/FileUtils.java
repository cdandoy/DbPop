package org.dandoy.dbpop.utils;

import java.io.File;

public class FileUtils {
    public static void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }

        if (!file.delete() && file.exists()) {
            throw new RuntimeException("Failed to delete " + file);
        }
    }

    public static void touch(File file) {
        if (!file.exists()) throw new RuntimeException("File does not exist:" + file);
        if (!file.setLastModified(System.currentTimeMillis())) {
            throw new RuntimeException("Failed to touch " + file);
        }
    }
}
