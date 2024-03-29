package org.dandoy.dbpop.cli;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.upload.Populator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static picocli.CommandLine.*;

/**
 * <pre>
 *   Usage: DbPop populate [-hvV] [-d=&lt;directory&gt;] [-j=&lt;dbUrl&gt;] -p=&lt;dbPassword&gt; [-u=&lt;dbUser&gt;] &lt;dataset&gt;...
 *   &lt;dataset&gt;...                   Datasets to load
 *   -d, --directory=&lt;directory&gt;    Dataset Directory
 *   -h, --help                     Show this help message and exit.
 *   -j, --jdbcurl=&lt;dbUrl&gt;          Database URL
 *   -p, --password=&lt;dbPassword&gt;    Database password
 *   -u, --username=&lt;dbUser&gt;        Database user
 *   -V, --version                  Print version information and exit.
 * </pre>
 */
@Command(name = "populate", description = "Populates the database with the content of the CSV files in the specified datasets")
@Slf4j
public class CommandPopulate implements Callable<Integer> {
    @Mixin
    private DatabaseOptions databaseOptions;

    @Option(names = {"-d", "--directory"}, description = "Dataset directory")
    String directory;

    @Parameters(paramLabel = "<dataset>", description = "Datasets", arity = "1..*")
    private final List<String> datasets = new ArrayList<>();

    @Override
    public Integer call() {
        try {
            long t0 = System.currentTimeMillis();
            int rowCount;
            UrlConnectionBuilder connectionBuilder = new UrlConnectionBuilder(databaseOptions.dbUrl, databaseOptions.dbUser, databaseOptions.dbPassword);
            try (Database database = Database.createDatabase(connectionBuilder)) {
                Populator populator = Populator.createPopulator(database, new File(directory));
                rowCount = populator.load(this.datasets);
                long t1 = System.currentTimeMillis();
                log.info("Loaded {} rows in {}ms", rowCount, t1 - t0);
                return 0;
            }
        } catch (Exception e) {
            log.error("Internal error", e);
            return 1;
        }
    }
}
