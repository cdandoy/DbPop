package org.dandoy.dbpopd.codechanges;

import ch.qos.logback.core.encoder.ByteArrayUtil;
import org.apache.commons.io.IOUtils;
import org.dandoy.dbpop.utils.ElapsedStopWatch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.dandoy.dbpopd.codechanges.HashCalculator.cleanSql;
import static org.dandoy.dbpopd.codechanges.HashCalculator.cleanSqlForHash;
import static org.junit.jupiter.api.Assertions.assertEquals;

class HashCalculatorTest {
    private static String readString(String filename) {
        try (InputStream inputStream = HashCalculatorTest.class.getResourceAsStream(filename)) {
            if (inputStream == null) throw new IOException("File not found: " + filename);
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testCleanStoreProcSql() {
        String text1 = readString("file1.txt");
        String text2 = readString("file2.txt");
        for (int i = 0; i < 100; i++) {
            cleanSql(text1 + " ".repeat(i));
        }

        ElapsedStopWatch stopWatch = new ElapsedStopWatch();
        for (int i = 0; i < 100; i++) {
            cleanSql(text1 + " ".repeat(i));
        }
        System.out.println("Done in " + stopWatch);
        assertEquals(cleanSqlForHash(text1), cleanSqlForHash(text2));

        byte[] hash1 = HashCalculator.getHash(text1);
        byte[] hash2 = HashCalculator.getHash(text2);
        assertEquals(ByteArrayUtil.toHexString(hash1), ByteArrayUtil.toHexString(hash2));
    }
}