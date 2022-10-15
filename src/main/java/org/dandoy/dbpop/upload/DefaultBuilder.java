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

    public SELF setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
        return self();
    }

    public String getDbUser() {
        return dbUser == null ? env.getString("username") : dbUser;
    }

    public SELF setDbUser(String dbUser) {
        this.dbUser = dbUser;
        return self();
    }

    public String getDbPassword() {
        return dbPassword == null ? env.getString("password") : dbPassword;
    }

    public SELF setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
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
