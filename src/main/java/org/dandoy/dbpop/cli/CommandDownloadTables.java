package org.dandoy.dbpop.cli;

import org.dandoy.dbpop.download.Downloader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static picocli.CommandLine.*;

@Command(name = "tables", description = "Download the specified tables")
public class CommandDownloadTables implements Callable<Integer> {
    @Mixin
    private DatabaseOptions databaseOptions;

    @Mixin
    private StandardOptions standardOptions;

    @Option(names = {"--dataset"}, description = "Dataset", required = true)
    private String dataset;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Parameters(paramLabel = "<tables>", description = "Tables", arity = "1..*")
    private final List<String> tables = new ArrayList<>();

    @Override
    public Integer call() {
        try (Downloader downloader = Downloader.builder()
                .setConnection(databaseOptions.dbUrl, databaseOptions.dbUser, databaseOptions.dbPassword)
                .setDirectory(standardOptions.directory)
                .setVerbose(standardOptions.verbose)
                .setDataset(dataset)
                .build()) {
            for (String table : tables) {
                downloader.download(table);
            }
            return 0;
        }
    }
}
