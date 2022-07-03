package org.dandoy.dbpop.cli;

import static picocli.CommandLine.Option;

public class DatabaseOptions {
    @Option(names = {"-j", "--jdbcurl"}, description = "Database URL")
     String dbUrl = "jdbc:sqlserver://localhost;database=tempdb;trustServerCertificate=true";

    @Option(names = {"-u", "--username"}, description = "Database user")
     String dbUser = "sa";

    @Option(names = {"-p", "--password"}, description = "Database password", required = true)
     String dbPassword;
}
