package org.dandoy.dbpopd.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class IOUtils {
    public static String readFully(File file) throws IOException {
        Path path = file.toPath();
        try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {
            StringBuilder sb = new StringBuilder();
            char[] buffer = new char[8 * 1024];
            while (true) {
                int read = bufferedReader.read(buffer, 0, buffer.length);
                if (read <= 0) break;
                sb.append(buffer, 0, read);
            }
            return sb.toString();
        }
    }
}
