package org.dandoy.dbpopd.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Slf4j
public class FileUtils {
    public static List<File> getFiles(File dir) {
        ArrayList<File> ret = new ArrayList<>();
        getFiles(dir, ret);
        return ret;
    }

    private static void getFiles(File dir, ArrayList<File> ret) {
        File[] files = dir.listFiles();
        if (files == null) {
            ret.add(dir);
        } else {
            for (File file : files) {
                getFiles(file, ret);
            }
        }
    }

    public static void deleteFiles(Collection<File> files) {
        for (File file : files) {
            if (!file.delete() && file.exists()) {
                log.error("Failed to delete " + file);
            }
        }
    }

    public static File toFile(File directory, String... parts) {
        return new File(directory, String.join("/", parts));
    }
}
