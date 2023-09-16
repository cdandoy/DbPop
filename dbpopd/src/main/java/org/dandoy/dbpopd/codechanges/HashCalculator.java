package org.dandoy.dbpopd.codechanges;

import jakarta.annotation.Nonnull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dandoy.dbpop.database.Database;

import javax.validation.constraints.NotNull;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

@Slf4j
public class HashCalculator {

    @NotNull
    static ObjectSignature getObjectSignature(long ts, String sql) {
        return new ObjectSignature(ts, HashCalculator.getHash(sql));
    }

    @NotNull
    static byte[] getHash(String sql) {
        String cleanSql = cleanSqlForHash(sql);
        byte[] bytes = cleanSql.getBytes(StandardCharsets.UTF_8);
        return getMessageDigest().digest(bytes);
    }

    static long getTimeDelta(Database database) {
        return database.getEpochTime() - new Date().getTime();
    }

    /**
     * Standardizes the SQL text.
     * TODO: Could do something more efficient.
     */
    public static String cleanSql(@Nonnull String sql) {
        return sql
                .replace("\t", " ")     // Replace the tabs with spaces
                .replace("\r\n", "\n")  // Windows
                .replace("\r", "\n")    // Mac
                .trim();                // Remove leading and trailing spaces
    }

    /**
     * Replace consecutive spaces with one space to fix tab expansion issues
     */
    private static String cleanSpaces(String line) {
        int pos = 0;
        while (true) {
            int i = line.indexOf("  ", pos);
            if (i == -1) break;
            line = line.substring(0, i + 1) + line.substring(i + 2);
            pos = i;
        }
        // Remove leading space
        if (line.startsWith(" ")) line = line.substring(1);

        // Remove trailing space
        if (line.endsWith(" ")) line = line.substring(0, line.length() - 1);
        return line;
    }

    /**
     * Standardizes the SQL text.
     */
    public static String cleanSqlForHash(@Nonnull String sql) {
        sql = sql
                .replace("\t", " ")     // Replace the tabs with spaces
                .replace("\r\n", "\n")  // Windows
                .replace("\r", "\n")    // Mac
                .toUpperCase();         // Remove leading and trailing spaces
        String[] lines = StringUtils.split(sql, "\n");
        return Arrays.stream(lines)
                .map(HashCalculator::cleanSpaces)                       // compact spaces
                .filter(line -> !(line.isEmpty() || line.equals(" ")))  // Remove empty lines
                .collect(Collectors.joining("\n"));
    }

    @SneakyThrows
    static MessageDigest getMessageDigest() {
        return MessageDigest.getInstance("SHA-256");
    }
}
