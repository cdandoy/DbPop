package org.dandoy.dbpop.cli;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.download.TableDownloader;
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

    @Option(names = {"-d", "--directory"}, description = "Dataset Directory")
    File directory;

    @Option(names = {"--dataset"}, description = "Dataset", required = true)
    private String dataset;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Parameters(paramLabel = "<tables>", description = "Tables", arity = "1..*")
    private final List<String> tables = new ArrayList<>();

    @Override
    public Integer call() {
        try (Database database = Database.createDatabase(new UrlConnectionBuilder(databaseOptions.dbUrl, databaseOptions.dbUser, databaseOptions.dbPassword))) {
            for (String table : tables) {
                List<String> split = StringUtils.split(table, '.');

                if (split.size() == 1) {            // database, no catalog: "master"
                    downloadDatabase(database, split.get(0));
                } else if (split.size() == 2) {      // database and catalog: "master.dbo"
                    downloadSchema(database, split.get(0), split.get(1));
                } else if (split.size() == 3) {      // database and catalog: "master.dbo.mytable"
                    downloadTable(database, split.get(0), split.get(1), split.get(2));
                } else {
                    throw new RuntimeException("Invalid database/schema/table: " + table);
                }
            }
            return 0;
        }
    }

    private void downloadDatabase(Database database, String catalog) {
        database.getSchemas(catalog)
                .stream()
                .flatMap(s -> database.getTableNames(catalog, s).stream())
                .forEach(tableName -> download(database, tableName));
    }

    private void downloadSchema(Database database, String catalog, String schema) {
        database
                .getTableNames(catalog, schema)
                .forEach(tableName -> download(database, tableName));
    }

    private void downloadTable(Database database, String catalog, String schema, String table) {
        TableName tableName = new TableName(catalog, schema, table);
        download(database, tableName);
    }

    private void download(Database database, TableName tableName) {
        try (TableDownloader tableDownloader = TableDownloader.builder()
                .setDatabase(database)
                .setDatasetsDirectory(directory)
                .setDataset(dataset)
                .setTableName(tableName)
                .build()) {
            tableDownloader.download();
        }
    }
}
