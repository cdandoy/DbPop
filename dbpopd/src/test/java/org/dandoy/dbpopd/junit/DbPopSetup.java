package org.dandoy.dbpopd.junit;

import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpop.tests.TestUtils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.MSSQLServerContainer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

@SuppressWarnings("rawtypes")
public class DbPopSetup implements BeforeAllCallback, BeforeEachCallback, ExtensionContext.Store.CloseableResource {
    public static final File TEMP_DIR = new File("../files/temp");
    static MSSQLServerContainer sourceContainer;
    static MSSQLServerContainer targetContainer;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (sourceContainer == null) {
            sourceContainer = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:latest").acceptLicense();
            sourceContainer.start();
            resetSourceDatabase();
        }

        if (targetContainer == null) {
            targetContainer = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:latest").acceptLicense();
            targetContainer.start();
        }

        setupConfigDirectory();
    }

    @Override
    public void close() {
        if (sourceContainer != null) {
            sourceContainer.stop();
            sourceContainer = null;
        }
        if (targetContainer != null) {
            targetContainer.stop();
            targetContainer = null;
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        setupConfigDirectory();

        resetTargetDatabase();
        DbPopTest dbPopTest = context.getTestClass().orElse(Object.class).getAnnotation(DbPopTest.class);
        if (dbPopTest != null && dbPopTest.withTargetTables()) {
            createTargetTables();
        }
    }

    private void setupConfigDirectory() throws IOException {
        TestUtils.delete(TEMP_DIR);
        try {
            FileUtils.copyDirectory(new File("../files/config"), new File("../files/temp"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        File file = new File(TEMP_DIR, "dbpop.properties");
        Properties properties = new Properties();
        if (file.exists()) {
            try (FileInputStream fileInputStream = new FileInputStream(file)) {
                properties.load(fileInputStream);
            }
        }
        properties.setProperty("SOURCE_JDBCURL", sourceContainer == null ? null : sourceContainer.getJdbcUrl());
        properties.setProperty("SOURCE_USERNAME", sourceContainer == null ? null : sourceContainer.getUsername());
        properties.setProperty("SOURCE_PASSWORD", sourceContainer == null ? null : sourceContainer.getPassword());
        properties.setProperty("TARGET_JDBCURL", targetContainer == null ? null : targetContainer.getJdbcUrl());
        properties.setProperty("TARGET_USERNAME", targetContainer == null ? null : targetContainer.getUsername());
        properties.setProperty("TARGET_PASSWORD", targetContainer == null ? null : targetContainer.getPassword());
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            properties.store(fileOutputStream, null);
        }
    }

    private void resetSourceDatabase() throws SQLException {
        try (Connection connection = sourceContainer.createConnection("")) {
            execute(connection, "DROP DATABASE IF EXISTS dbpop");
            execute(connection, "CREATE DATABASE dbpop");
            SqlExecutor.execute(
                    connection,
                    "/mssql/drop.sql",
                    "/mssql/create.sql",
                    "/mssql/insert_data.sql"
            );
        }
    }

    public void resetTargetDatabase() throws SQLException {
        try (Connection connection = targetContainer.createConnection("")) {
            execute(connection, "DROP DATABASE IF EXISTS dbpop");
            execute(connection, "CREATE DATABASE dbpop");
        }
    }

    private void createTargetTables() {
        try (Connection connection = targetContainer.createConnection("")) {
            SqlExecutor.execute(
                    connection,
                    "/mssql/drop.sql",
                    "/mssql/create.sql"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private static void execute(Connection connection, String stmt) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(stmt)) {
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute " + stmt, e);
        }
    }
}
