package org.dandoy.dbpop.upload;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.cli.DatabaseOptions;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.utils.Env;

import java.io.File;

@Slf4j
public abstract class DefaultBuilder<SELF extends DefaultBuilder<?, ?>, T> {
    private Env env;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private Boolean verbose;

    protected DefaultBuilder() {
        env = Env.createEnv();
    }

    DefaultBuilder(File directory) {
        env = Env.createEnv(directory);
    }

    public abstract T build();

    @SuppressWarnings("unchecked")
    private SELF self() {
        return (SELF) this;
    }

    public SELF setEnvironment(String environment) {
        env = env.environment(environment);
        return self();
    }

    public String getDbUrl() {
        return dbUrl == null ? env.getString("jdbcurl") : dbUrl;
    }

    public String getDbUser() {
        return dbUser == null ? env.getString("username") : dbUser;
    }

    public String getDbPassword() {
        return dbPassword == null ? env.getString("password") : dbPassword;
    }

    public boolean isVerbose() {
        return verbose != null ? verbose : Boolean.parseBoolean(env.getString("verbose", "false"));
    }

    /**
     * Enables verbose logging on System.out
     *
     * @param verbose verbose on/off
     * @return this
     */
    public SELF setVerbose(boolean verbose) {
        this.verbose = verbose;
        return self();
    }

    public ConnectionBuilder getConnectionBuilder() {
        return new UrlConnectionBuilder(
                getDbUrl(),
                getDbUser(),
                getDbPassword()
        );
    }

    public SELF setConnection(DatabaseOptions databaseOptions) {
        dbUrl = databaseOptions.dbUrl;
        dbUser = databaseOptions.dbUser;
        dbPassword = databaseOptions.dbPassword;
        return self();
    }
}
