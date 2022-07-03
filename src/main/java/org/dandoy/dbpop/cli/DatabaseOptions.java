package org.dandoy.dbpop.cli;

import static picocli.CommandLine.Option;

public class DatabaseOptions {
    @Option(names = {"-j", "--jdbcurl"}, description = "Database URL")
    public String dbUrl;

    @Option(names = {"-u", "--username"}, description = "Database user")
    public String dbUser;

    @Option(names = {"-p", "--password"}, description = "Database password")
    public String dbPassword;
}
