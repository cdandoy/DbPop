package org.dandoy.dbpopd.utils;

import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpop.tests.TestUtils;
import org.dandoy.dbpopd.ConfigurationService;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DbPopTestUtils {

    /**
     * Copies the .../files/config to .../files/temp
     * Drops and re-creates the source database tables and inserts the data.
     * Drops the target database. Doesn't create it because we want to eventually handle that with /code/
     */
    public static void setUp() {
        TestUtils.delete(new File("../files/config/code")); // Just to be sure, it shouldn't be there
        TestUtils.delete(new File("../files/temp"));
        try {
            FileUtils.copyDirectory(new File("../files/config"), new File("../files/temp"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ConfigurationService configurationService = new ConfigurationService("../files/temp", null, null, null);

        try {
            try (Connection connection = configurationService.getSourceConnectionBuilder().createConnection()) {
                SqlExecutor.execute(
                        connection,
                        "/mssql/drop.sql",
                        "/mssql/create.sql",
                        "/mssql/insert_data.sql"
                );
            }

            try (Connection connection = configurationService.getTargetConnectionBuilder().createConnection()) {
                SqlExecutor.execute(connection, "/mssql/drop.sql");
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE IF EXISTS master.dbo.dbpop_timestamps");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
