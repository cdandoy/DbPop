package org.dandoy.dbpop.cli;

import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.download.Downloader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import static picocli.CommandLine.*;

@Command(name = "schemas", description = "Download the specified schemas")
public class CommandDownloadSchema implements Callable<Integer> {
    private static final Pattern SPLIT_PATTERN = Pattern.compile("\\.");
    @Mixin
    private DatabaseOptions databaseOptions;

    @Mixin
    private StandardOptions standardOptions;

    @Option(names = {"--dataset"}, description = "Dataset", required = true)
    private String dataset;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Parameters(paramLabel = "<schemas>", description = "Schemas", arity = "1..*")
    private final List<String> schemas = new ArrayList<>();

    @Override
    public Integer call() {
        try (Downloader downloader = Downloader.builder()
                .setEnvironment(standardOptions.environment)
                .setConnection(databaseOptions)
                .setDirectory(standardOptions.directory)
                .setVerbose(standardOptions.verbose)
                .setDataset(dataset)
                .build()) {
            Connection connection = downloader.getConnection();
            try (Database database = Database.createDatabase(connection)) {
                Set<TableName> tableNames = new HashSet<>();
                for (String schema : schemas) {
                    String[] split = SPLIT_PATTERN.split(schema);
                    if (split.length == 1) {
                        tableNames.addAll(
                                database.getTableNames(connection.getCatalog(), split[0])
                        );
                    } else if (split.length == 2) {
                        tableNames.addAll(
                                database.getTableNames(split[0], split[1])
                        );
                    } else {
                        throw new RuntimeException("Invalid schema " + schema);
                    }
                }
                tableNames.forEach(downloader::download);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return 0;
        }
    }
}
