package org.dandoy.dbpop.download;

import org.dandoy.LocalCredentials;
import org.dandoy.TestUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.upload.Populator;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

class ExecutionPlanTest {
    @Test
    void name() throws SQLException {
        File datasetsDirectory = new File("src/test/resources/mssql");
        TestUtils.delete(new File(datasetsDirectory, "download"));
        LocalCredentials localCredentials = LocalCredentials.from("mssql");

        try (Populator populator = localCredentials
                .populator()
                .setDirectory(datasetsDirectory)
                .build()) {
            populator.load("invoices");
        }

        try (Connection connection = localCredentials.createConnection()) {
            try (Database database = Database.createDatabase(connection)) {
                try (ExecutionPlan executionPlan = new ExecutionPlan(database, datasetsDirectory, "download")) {
                    TableName tableName = new TableName("master", "dbo", "invoices");
                    executionPlan.build(tableName, Collections.emptyList());
                    executionPlan.download(Collections.emptySet());
                }
            }
        }

        TestUtils.delete(new File(datasetsDirectory, "download"));
    }
}