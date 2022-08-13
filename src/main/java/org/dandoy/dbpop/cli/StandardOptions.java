package org.dandoy.dbpop.cli;

import static picocli.CommandLine.Option;

public class StandardOptions {
    @Option(names = {"-e", "--environment"}, description = "Environment")
    String environment;

    @Option(names = {"-v", "--verbose"}, description = "Verbose")
    boolean verbose;
}
