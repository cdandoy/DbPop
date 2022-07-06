package org.dandoy;

import org.dandoy.dbpop.utils.Env;

import java.io.File;
import java.util.Objects;

/**
 * Used by JUnit: <pre>@EnabledIf("org.dandoy.TestUtils#hasDatabaseSetup")</pre>
 */
@SuppressWarnings("unused")
public class TestUtils {
    private static final Env env = Objects.requireNonNull(Env.createEnv());

    public static boolean hasDatabaseSetup() {
        return env.getString("jdbcurl").startsWith("jdbc:");
    }

    public static boolean isSqlServer() {
        return env.environment(null).getString("jdbcurl").startsWith("jdbc:sqlserver:");
    }

    public static boolean hasSqlServer() {
        return env.environment("mssql").getString("jdbcurl").startsWith("jdbc:sqlserver:");
    }

    public static boolean isPostgres() {
        return env.environment(null).getString("jdbcurl").startsWith("jdbc:postgresql:");
    }

    public static boolean hasPostgres() {
        return env.environment("pgsql").getString("jdbcurl").startsWith("jdbc:postgresql:");
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
