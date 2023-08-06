package org.dandoy.dbpop.database;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Pattern;

@Slf4j
@Getter
public class UrlConnectionBuilder implements ConnectionBuilder {
    public static final int WAIT_COUNT = 30;    // Wait N times for a connection
    public static final int WAIT_TIME = 1000;   // Wait N millis between each retry
    private static final Pattern JDBC_URL_PARSER = Pattern.compile("jdbc:sqlserver://(\\w+)(:(\\d+))?(;.*)");
    private final String url;
    private final String username;
    private final String password;

    public UrlConnectionBuilder(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @Override
    public String toString() {
        return username + "@" + url;
    }

    @Override
    public Connection createConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }
}
