package org.dandoy.dbpopd.codechanges;

import ch.qos.logback.core.encoder.ByteArrayUtil;
import org.apache.commons.io.IOUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseProxy;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.utils.ElapsedStopWatch;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.dandoy.dbpopd.codechanges.HashCalculator.cleanSql;
import static org.junit.jupiter.api.Assertions.assertEquals;

class HashCalculatorTest {
    private Collection<ObjectIdentifier> exclude(Collection<ObjectIdentifier> a, Collection<ObjectIdentifier> b) {
        Set<ObjectIdentifier> ret = new HashSet<>(a);
        ret.removeAll(b);
        return ret;
    }

    private Collection<ObjectIdentifier> intersect(Collection<ObjectIdentifier> a, Collection<ObjectIdentifier> b) {
        Set<ObjectIdentifier> ret = new HashSet<>(a);
        ret.retainAll(b);
        return ret;
    }

    @Test
    void name() {
        File codeDirectory = new File("D:\\git\\dbpop\\ws1\\files\\config\\code");
        UrlConnectionBuilder connectionBuilder = new UrlConnectionBuilder("jdbc:sqlserver://localhost:2433;database=tempdb;trustServerCertificate=true", "sa", "GlobalTense1010");
        try (DatabaseProxy database = Database.createDatabase(connectionBuilder)) {
            Map<ObjectIdentifier, ObjectSignature> fileSignatures = HashCalculator.captureSignatures(codeDirectory);
            Map<ObjectIdentifier, ObjectSignature> databaseSignatures = HashCalculator.captureSignatures(database);
            exclude(fileSignatures.keySet(), databaseSignatures.keySet()).forEach(objectIdentifier -> System.out.println("Missing db"));
            exclude(databaseSignatures.keySet(), fileSignatures.keySet()).forEach(objectIdentifier -> System.out.println("Missing file"));
            intersect(fileSignatures.keySet(), databaseSignatures.keySet()).forEach(objectIdentifier -> {
                ObjectSignature fileSignature = fileSignatures.get(objectIdentifier);
                ObjectSignature databaseSignature = databaseSignatures.get(objectIdentifier);
                if (!Arrays.equals(fileSignature.hash(), databaseSignature.hash())) {
                    System.out.println("Different signatures: " + objectIdentifier);
                }
            });
        }
    }

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
        assertEquals(cleanSql(text1), cleanSql(text2));

        byte[] hash1 = HashCalculator.getHash("VIEW", text1);
        byte[] hash2 = HashCalculator.getHash("VIEW", text2);
        assertEquals(ByteArrayUtil.toHexString(hash1), ByteArrayUtil.toHexString(hash2));
    }
}