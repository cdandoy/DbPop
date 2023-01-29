package org.dandoy.dbpopd.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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

     static String toFileName(String in) {
        // Try not to create a new string: look at each character and if one is bad, then calculate the new string
        String bad = "@$%&\\/:*?\"'<>|~`#^+={}[];!";
        char[] chars = in.toCharArray();
         for (int i = 0; i < chars.length; i++) {
             char c = chars[i];
             if (bad.indexOf(c) >= 0) {
                 StringBuilder sb = new StringBuilder(in.substring(0, i));
                 for (int j = i; j < chars.length; j++) {
                     char c2 = chars[j];
                     if (bad.indexOf(c2) >= 0) {
                         sb.append('_');
                     } else {
                         sb.append(c2);
                     }
                 }
                 return sb.toString();
             }
         }
        return in;
    }

    public static File toFile(File directory, String... parts) {
        return new File(
                directory,
                Arrays.stream(parts)
                        .map(FileUtils::toFileName)
                        .collect(Collectors.joining("/"))
        );
    }
}
