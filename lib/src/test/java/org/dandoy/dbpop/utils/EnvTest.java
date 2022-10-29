package org.dandoy.dbpop.utils;

import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class EnvTest {
    @Test
    void name() throws IOException {
        File directory = Files.createTempDirectory("EnvTest").toFile();
        try {
            createPropertyFile(directory);
            Env env = Env.createEnv(directory);
            assertNotNull(env);
            assertEquals("un", env.getString("one"));
            assertEquals("deux", env.getString("two"));
            assertEquals("uno", env.environment("spanish").getString("one"));
            assertEquals("dos", env.environment("spanish").getString("two"));
            assertEquals("een", env.environment("deutch").getString("one"));
            assertEquals("twee", env.environment("deutch").getString("two"));
        } finally {
            delete(directory);
        }
    }

    public static void createPropertyFile(File directory) throws IOException {
        createFile(new File(directory, "dbpop.properties"),
                "one=un",
                "two=deux",
                "spanish.one=uno",
                "spanish.two=dos",
                "deutch.one=een",
                "deutch.two=twee"
        );
    }

    public static void createFile(File file, String... lines) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file.toPath())) {
            for (String line : lines) {
                writer.append(line).append("\n");
            }
        }
    }

    public static void delete(File parent) {
        File[] files = parent.listFiles();
        if (files != null) {
            for (File file : files) {
                delete(file);
            }
        }
        if (!parent.delete())
            throw new RuntimeException("Failed to delete " + parent);
    }
}