package org.dandoy.dbpop.upload;

import org.dandoy.dbpop.cli.DatabaseOptions;
import org.dandoy.dbpop.utils.EnvTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

class DefaultBuilderTest {
    @Test
    void name() throws IOException {
        File directory = Files.createTempDirectory("EnvTest").toFile();
        try {
            EnvTest.createFile(new File(directory, "dbpop.properties"),
                    "jdbcurl=jdbc:postgresql://localhost:5432/default",
                    "username=default-username",
                    "password=default-password",
                    "verbose=true",
                    "mssql.jdbcurl=jdbc:sqlserver://localhost",
                    "mssql.username=mssql-username",
                    "mssql.password=mssql-password",
                    "mssql.verbose=false",
                    "pgsql.jdbcurl=jdbc:postgresql://localhost:5432/pgsql",
                    "pgsql.username=pgsql-username",
                    "pgsql.password=pgsql-password"
            );

            Builder builder = new Builder(directory)
                    .setConnection(new DatabaseOptions());
            assertEquals("jdbc:postgresql://localhost:5432/default", builder.getDbUrl());
            assertEquals("default-username", builder.getDbUser());
            assertEquals("default-password", builder.getDbPassword());

            builder.setEnvironment("mssql");
            assertEquals("jdbc:sqlserver://localhost", builder.getDbUrl());
            assertEquals("mssql-username", builder.getDbUser());
            assertEquals("mssql-password", builder.getDbPassword());

            builder.setEnvironment("pgsql");
            assertEquals("jdbc:postgresql://localhost:5432/pgsql", builder.getDbUrl());
            assertEquals("pgsql-username", builder.getDbUser());
            assertEquals("pgsql-password", builder.getDbPassword());

        } finally {
            delete(directory);
        }
    }

    private static void createFile(File file, String... lines) throws IOException {
        EnvTest.createFile(file, lines);
    }

    private void delete(File parent) {
        EnvTest.delete(parent);
    }

    public static class Builder extends DefaultBuilder<Builder, Void> {
        Builder(File directory) {
            super(directory);
        }

        @Override
        public Void build() {
            return null;
        }
    }
}