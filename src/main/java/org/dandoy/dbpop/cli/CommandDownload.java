package org.dandoy.dbpop.cli;

import static picocli.CommandLine.Command;

@Command(name = "download", description = "Download data to CSV files",
        subcommands = {
                CommandDownloadTables.class,
                CommandDownloadSchema.class,
        })
public class CommandDownload {
}
