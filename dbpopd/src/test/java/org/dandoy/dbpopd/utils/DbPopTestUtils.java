package org.dandoy.dbpopd.utils;

import io.micronaut.context.event.ApplicationEventPublisher;
import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpop.tests.TestUtils;
import org.dandoy.dbpopd.config.DatabasesConfigurationService;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DbPopTestUtils {

    /**
     * Copies the .../files/config to .../files/temp
     * Drops and re-creates the source database tables and inserts the data.
     * Drops the target database. Doesn't create it because we want to eventually handle that with /code/
     */
    public static void setUp() {
        TestUtils.delete(new File("../files/config/code")); // Just to be sure, it shouldn't be there
        TestUtils.delete(new File("../files/config/datasets")); // Just to be sure, it shouldn't be there
        TestUtils.delete(new File("../files/temp"));
        try {
            FileUtils.copyDirectory(new File("../files/config"), new File("../files/temp"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DatabasesConfigurationService databasesConfigurationService = new DatabasesConfigurationService("../files/temp",
                ApplicationEventPublisher.noOp(),
                ApplicationEventPublisher.noOp()
        );

        try {
            try (Connection connection = databasesConfigurationService._createSourceConnectionBuilder().createConnection()) {
                execute(connection, "DROP DATABASE IF EXISTS dbpop");
                execute(connection, "CREATE DATABASE dbpop");
                SqlExecutor.execute(
                        connection,
                        "/mssql/drop.sql",
                        "/mssql/create.sql",
                        "/mssql/insert_data.sql"
                );
            }

            try (Connection connection = databasesConfigurationService._createTargetConnectionBuilder().createConnection()) {
                execute(connection, "DROP DATABASE IF EXISTS dbpop");
                execute(connection, "CREATE DATABASE dbpop");
            }
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
