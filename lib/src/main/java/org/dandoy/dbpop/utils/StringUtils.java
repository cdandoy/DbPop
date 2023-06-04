package org.dandoy.dbpop.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StringUtils {
    public static List<String> split(String name, char splitChar) {
        List<String> ret = new ArrayList<>(3);
        StringBuilder sb = new StringBuilder();
        for (char c : name.toCharArray()) {
            if (c == splitChar) {
                ret.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        ret.add(sb.toString());
        return ret;
    }

    public static String removeEnd(String s, String end) {
        if (s.endsWith(end)) {
            return s.substring(0, s.length() - end.length());
        }
        return s;
    }

    public static String repeat(String s, int count, String separator) {
        return IntStream.range(0, count)
                .mapToObj(it -> s)
                .collect(Collectors.joining(separator));
    }

    public static String normalizeEOL(String s) {
        if (s == null) return null;
        return s
                .replace("\r\n", "\n")  // Windows
                .replace("\r", "\n");   // Mac
    }
}
