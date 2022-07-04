package org.dandoy;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;

/**
 * Used by JUnit: <pre>@EnabledIf("org.dandoy.TestUtils#hasDatabaseSetup")</pre>
 */
@SuppressWarnings("unused")
public class TestUtils {
    public static boolean hasDatabaseSetup() {
        return getJdbcUrl().startsWith("jdbc:");
    }

    public static boolean isSqlServer() {
        return getJdbcUrl().startsWith("jdbc:sqlserver:");
    }

    public static boolean isPostgres() {
        return getJdbcUrl().startsWith("jdbc:postgresql:");
    }

    private static String getJdbcUrl() {
        Properties properties = new Properties();
        String userHome = System.getProperty("user.home");
        if (userHome == null) throw new RuntimeException("Cannot find your home directory");
        File propertyFile = new File(userHome, "dbpop.properties");
        if (propertyFile.exists()) {
            try (BufferedReader bufferedReader = Files.newBufferedReader(propertyFile.toPath(), StandardCharsets.UTF_8)) {
                properties.load(bufferedReader);
                return properties.getProperty("jdbcurl", "");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return "";
    }

    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
