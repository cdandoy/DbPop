package org.dandoy.dbpopd.utils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public class IOUtils {
    public static String toString(File file) throws IOException {
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

    public static byte[] toBytes(File file) throws IOException {
        try (InputStream inputStream = new FileInputStream(file)) {
            byte[] ret = new byte[(int) file.length()];
            int read = inputStream.read(ret, 0, ret.length);
            if (read != ret.length) throw new RuntimeException("I did not expect that: Expected to read %d, but read %d".formatted(ret.length, read));
            return ret;
        }
    }
}
