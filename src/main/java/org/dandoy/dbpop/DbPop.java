package org.dandoy.dbpop;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "DbPop", version = "DbPop 0.1", mixinStandardHelpOptions = true)
public class DbPop implements Callable<Integer> {
    @Option(names = {"-j", "--jdbcurl"}, description = "Database URL")
    private String dbUrl = "jdbc:sqlserver://localhost;database=tempdb;trustServerCertificate=true";

    @Option(names = {"-u", "--username"}, description = "Database user")
    private String dbUser = "sa";

    @Option(names = {"-p", "--password"}, description = "Database password", required = true)
    private String dbPassword;

    @Option(names = {"-d", "--directory"}, description = "Dataset Directory")
    private File directory = new File(".");

    @Option(names = {"-v", "--verbose"}, description = "Verbose")
    private boolean verbose;

    @Parameters(paramLabel = "<dataset>", description = "Datasets", arity = "1..*")
    private final List<String> datasets = new ArrayList<>();

    public static void main(String[] args) {
        int exitCode = likeMain(args);
        System.exit(exitCode);
    }

    /**
     * Used for tests only
     */
    static int likeMain(String[] args) {
        return new CommandLine(new DbPop()).execute(args);
    }

    @Override
    public Integer call() {
        try {
            long t0 = System.currentTimeMillis();
            int rowCount;
            try (Populator populator = Populator.builder()
                    .setConnection(dbUrl, dbUser, dbPassword)
                    .setDirectory(directory)
                    .setVerbose(verbose)
                    .build()) {
                rowCount = populator.load(this.datasets);
            }
            long t1 = System.currentTimeMillis();
            System.out.printf("Loaded %d rows in %dms%n", rowCount, t1 - t0);
            return 0;
        } catch (Exception e) {
            if (verbose) {
                e.printStackTrace(System.err);
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
                System.err.println(indent + t.getMessage());
                indent.append("  ");
            }
            t = t.getCause();
        }
    }
}
