package org.dandoy.dbpop;

import org.dandoy.dbpop.cli.CommandDownload;
import org.dandoy.dbpop.cli.CommandPopulate;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * CLI main class.
 */
@Command(
        name = "DbPop",
        version = "DbPop 0.1",
        mixinStandardHelpOptions = true,
        subcommands = {
                CommandLine.HelpCommand.class,
                CommandPopulate.class,
                CommandDownload.class
        }
)
public class DbPop {
    public static void main(String[] args) {
        int exitCode = likeMain(args);
        System.exit(exitCode);
    }

    /**
     * Used for tests only
     * @param args the command line arguments
     * @return the exit code
     */
    @SuppressWarnings("InstantiationOfUtilityClass")
    public static int likeMain(String... args) {
        DbPop dbPop = new DbPop();
        return new CommandLine(dbPop)
                .setUsageHelpAutoWidth(false)
                .setUsageHelpWidth(128)
                .setUsageHelpLongOptionsMaxWidth(128 - 20)
                .execute(args);
    }
}
