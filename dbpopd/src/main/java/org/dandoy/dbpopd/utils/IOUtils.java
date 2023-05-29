package org.dandoy.dbpopd.utils;

import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class IOUtils {
    @SneakyThrows
    public static String toString(File file) {
        return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    }

    public static File toCanonical(File file) {
        File absoluteFile = file.getAbsoluteFile();
        try {
            return absoluteFile.getCanonicalFile();
        } catch (IOException e) {
            return absoluteFile;
        }
    }
}
