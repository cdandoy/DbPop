package org.dandoy.dbpop.tests.mssql;

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
public class DbPopContainerSetup implements BeforeAllCallback, BeforeEachCallback, ExtensionContext.Store.CloseableResource {
    public static final File TEMP_DIR = new File("../files/temp");
    private static MSSQLServerContainer sourceContainer;
    private static MSSQLServerContainer targetContainer;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        DbPopContainerTest dbPopContainerTest = context.getTestClass().orElse(Object.class).getAnnotation(DbPopContainerTest.class);
        if (dbPopContainerTest != null && dbPopContainerTest.source()) {
            if (sourceContainer == null) {
                sourceContainer = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:latest").acceptLicense();
                sourceContainer.start();
                resetSourceDatabase();
            }
        }

        if (dbPopContainerTest != null && dbPopContainerTest.target()) {
            if (targetContainer == null) {
                targetContainer = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:latest").acceptLicense();
                targetContainer.start();
            }
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

        if (targetContainer != null) {
            resetTargetDatabase();
            DbPopContainerTest dbPopContainerTest = context.getTestClass().orElse(Object.class).getAnnotation(DbPopContainerTest.class);
            if (dbPopContainerTest != null && dbPopContainerTest.withTargetTables()) {
                createTargetTables();
            }
        }
    }

    private void setupConfigDirectory() throws IOException {
        TestUtils.delete(TEMP_DIR);
        if (!TEMP_DIR.mkdirs() && !TEMP_DIR.isDirectory()) {
            throw new RuntimeException("Failed to create " + TEMP_DIR);
        }

        File file = new File(TEMP_DIR, "dbpop.properties");
        Properties properties = new Properties();
        if (file.exists()) {
            try (FileInputStream fileInputStream = new FileInputStream(file)) {
                properties.load(fileInputStream);
            }
        }
        if (sourceContainer != null) {
            properties.setProperty("SOURCE_JDBCURL", sourceContainer.getJdbcUrl());
            properties.setProperty("SOURCE_USERNAME", sourceContainer.getUsername());
            properties.setProperty("SOURCE_PASSWORD", sourceContainer.getPassword());
        } else {
            properties.remove("SOURCE_JDBCURL");
            properties.remove("SOURCE_USERNAME");
            properties.remove("SOURCE_PASSWORD");
        }
        if (targetContainer != null) {
            properties.setProperty("TARGET_JDBCURL", targetContainer.getJdbcUrl());
            properties.setProperty("TARGET_USERNAME", targetContainer.getUsername());
            properties.setProperty("TARGET_PASSWORD", targetContainer.getPassword());
        } else {
            properties.remove("TARGET_JDBCURL");
            properties.remove("TARGET_USERNAME");
            properties.remove("TARGET_PASSWORD");
        }
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

    private void resetTargetDatabase() throws SQLException {
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

    public static MSSQLServerContainer<?> getSourceContainer() {
        return sourceContainer;
    }

    public static MSSQLServerContainer<?> getTargetContainer() {
        return targetContainer;
    }
}
