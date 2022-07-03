package org.dandoy.dbpop.cli;

import java.io.File;

import static picocli.CommandLine.Option;

public class StandardOptions {
    @Option(names = {"-d", "--directory"}, description = "Dataset Directory")
    File directory;

    @Option(names = {"-v", "--verbose"}, description = "Verbose")
    boolean verbose;
}
