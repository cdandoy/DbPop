package org.dandoy;

import org.apache.commons.io.IOUtils;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.test.SqlServerTests;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TestUtils {
    public static final TableName invoices = new TableName("master", "dbo", "invoices");
    public static final TableName invoiceDetails = new TableName("master", "dbo", "invoice_details");
    public static final TableName customers = new TableName("master", "dbo", "customers");
    public static final TableName products = new TableName("master", "dbo", "products");

    public static boolean hasMssql() {
        return hasSourceMssql() && hasTargetMssql();
    }

    public static boolean hasSourceMssql() {
        return LocalCredentials.from("mssql").sourceConnectionBuilder() != null;
    }

    public static boolean hasTargetMssql() {
        return LocalCredentials.from("mssql").targetConnectionBuilder() != null;
    }

    public static boolean hasPgsql() {
        return hasSourcePgsql() && hasTargetPgsql();
    }

    public static boolean hasSourcePgsql() {
        return LocalCredentials.from("pgsql").sourceConnectionBuilder() != null;
    }

    public static boolean hasTargetPgsql() {
        return LocalCredentials.from("pgsql").targetConnectionBuilder() != null;
    }

    public static void prepareMssqlSource() {
        LocalCredentials
                .from("mssql")
                .executeSource(
                        "drop_tables.sql",
                        "create_tables.sql",
                        "insert_data.sql"
                );
    }

    public static void prepareMssqlTarget() {
        LocalCredentials
                .from("mssql")
                .executeTarget(
                        "drop_tables.sql",
                        "create_tables.sql"
                );
    }

    public static void executeTargetSql(String environment, String... paths) {
        try (Connection connection = LocalCredentials
                .from(environment)
                .createTargetConnection()
        ) {
            for (String path : paths) {
                try (InputStream resourceAsStream = SqlServerTests.class.getResourceAsStream(path)) {
                    if (resourceAsStream == null) throw new RuntimeException("Script not found: " + path);
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream, StandardCharsets.UTF_8))) {
                        String sql = IOUtils.toString(reader);
                        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                            preparedStatement.execute();
                        } catch (SQLException e) {
                            throw new RuntimeException("Failed to execute \n%s".formatted(sql), e);
                        }
                    }
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void delete(File file) {
        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                delete(f);
            }
        }
        if (!file.delete() && file.exists()) {
            throw new RuntimeException("Failed to delete " + file);
        }
    }
}
