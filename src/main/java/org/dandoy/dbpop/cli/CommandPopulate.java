package org.dandoy.dbpop.cli;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.upload.Populator;

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
 *   -v, --verbose                  Verbose
 *   -V, --version                  Print version information and exit.
 * </pre>
 */
@Command(name = "populate", description = "Populates the database with the content of the CSV files in the specified datasets")
@Slf4j
public class CommandPopulate implements Callable<Integer> {
    @Mixin
    private DatabaseOptions databaseOptions;

    @Mixin
    private StandardOptions standardOptions;

    @Option(names = {"--path"}, description = "Dataset path")
    String path;

    @Parameters(paramLabel = "<dataset>", description = "Datasets", arity = "1..*")
    private final List<String> datasets = new ArrayList<>();

    @Override
    public Integer call() {
        try {
            long t0 = System.currentTimeMillis();
            int rowCount;
            try (Populator populator = Populator.builder()
                    .setEnvironment(standardOptions.environment)
                    .setConnection(databaseOptions)
                    .setPath(path)
                    .setVerbose(standardOptions.verbose)
                    .build()) {
                rowCount = populator.load(this.datasets);
            }
            long t1 = System.currentTimeMillis();
            log.info("Loaded {} rows in {}ms", rowCount, t1 - t0);
            return 0;
        } catch (Exception e) {
            if (standardOptions.verbose) {
                log.error("Internal error", e);
            } else {
                printCauses(e);
            }
            return 1;
        }
    }

    private void printCauses(Throwable t) {
        StringBuilder indent = new StringBuilder();
        while (t != null) {
            if (t.getMessage() != null) {
                log.error(indent + t.getMessage());
                indent.append("  ");
            }
            t = t.getCause();
        }
    }
}
