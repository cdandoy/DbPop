package org.dandoy.dbpop.cli;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.download.Downloader;
import org.dandoy.dbpop.utils.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static picocli.CommandLine.*;

@Command(name = "download", description = "Download data to CSV files")
@Slf4j
public class CommandDownload implements Callable<Integer> {
    @Mixin
    private DatabaseOptions databaseOptions;

    @Mixin
    private StandardOptions standardOptions;

    @Option(names = {"-d", "--directory"}, description = "Dataset Directory")
    File directory;

    @Option(names = {"--dataset"}, description = "Dataset", required = true)
    private String dataset;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Parameters(paramLabel = "<tables>", description = "Tables", arity = "1..*")
    private final List<String> tables = new ArrayList<>();

    @Override
    public Integer call() {
        try (Downloader downloader = Downloader.builder()
                .setDbUrl(databaseOptions.dbUrl)
                .setDbUser(databaseOptions.dbUser)
                .setDbPassword(databaseOptions.dbPassword)
                .setDirectory(directory)
                .setDataset(dataset)
                .build()) {
            for (String table : tables) {
                List<String> split = StringUtils.split(table, '.');

                if (split.size() == 1) {            // database, no catalog: "master"
                    downloadDatabase(downloader, split.get(0));
                } else if (split.size() == 2) {      // database and catalog: "master.dbo"
                    downloadSchema(downloader, split.get(0), split.get(1));
                } else if (split.size() == 3) {      // database and catalog: "master.dbo.mytable"
                    downloadTable(downloader, split.get(0), split.get(1), split.get(2));
                } else {
                    throw new RuntimeException("Invalid database/schema/table: " + table);
                }
            }
            return 0;
        }
    }

    private static void downloadDatabase(Downloader downloader, String catalog) {
        Database database = downloader.getDatabase();
        database.getSchemas(catalog)
                .stream()
                .flatMap(s -> database.getTableNames(catalog, s).stream())
                .forEach(downloader::download);
    }

    private static void downloadSchema(Downloader downloader, String catalog, String schema) {
        downloader.getDatabase()
                .getTableNames(catalog, schema)
                .forEach(downloader::download);
    }

    private void downloadTable(Downloader downloader, String catalog, String schema, String table) {
        TableName tableName = new TableName(catalog, schema, table);
        downloader.download(tableName);
    }
}
