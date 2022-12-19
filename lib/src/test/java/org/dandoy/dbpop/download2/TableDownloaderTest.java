package org.dandoy.dbpop.download2;

import org.dandoy.LocalCredentials;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

@EnabledIf("org.dandoy.TestUtils#hasSqlServer")
class TableDownloaderTest {
    @Test
    void name() throws SQLException {
        try (Connection connection = LocalCredentials.from("mssql").createConnection()) {
            Database database = Database.createDatabase(connection);
            File datasetsDirectory = new File("src/test/resources/mssql");
            String dataset = "download";
            TableDownloader tableDownloader = TableDownloader.createTableDownloader(database, datasetsDirectory, dataset, new TableName("master", "dbo", "customers"));
            Set<List<Object>> pks = Set.of(List.of(101));
            tableDownloader.download(pks);
        }
    }
}