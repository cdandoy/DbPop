package org.dandoy;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;

/**
 * Used by JUnit: <pre>@EnabledIf("org.dandoy.TestEnv#hasDatabaseSetup")</pre>
 */
@SuppressWarnings("unused")
public class TestEnv {
    @SuppressWarnings("unused")
    public static boolean hasDatabaseSetup() {
        try {
            Properties properties = readDbPop();
            return properties.getProperty("jdbcurl") != null;
        } catch (Exception e) {
            return false;
        }
    }

    public static Properties readDbPop() {
        String userHome = System.getProperty("user.home");
        if (userHome == null) throw new RuntimeException("Cannot find your home directory");
        File propertyFile = new File(userHome, "dbpop.properties");
        if (propertyFile.exists()) {
            Properties properties = new Properties();
            try (BufferedReader bufferedReader = Files.newBufferedReader(propertyFile.toPath(), StandardCharsets.UTF_8)) {
                properties.load(bufferedReader);
                return properties;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Could not find connection properties");
        }
    }
}
