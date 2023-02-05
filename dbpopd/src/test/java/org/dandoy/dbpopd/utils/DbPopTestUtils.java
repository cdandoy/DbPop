package org.dandoy.dbpopd.utils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.tests.SqlExecutor;
import org.dandoy.dbpop.tests.TestUtils;
import org.dandoy.dbpopd.ConfigurationService;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Singleton
public class DbPopTestUtils {
    @Inject
    ConfigurationService configurationService;

    public void setUp() throws SQLException {
        TestUtils.delete(new File("../files/temp"));
        try {
            FileUtils.copyDirectory(new File("../files/config"), new File("../files/temp"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
    }
}
