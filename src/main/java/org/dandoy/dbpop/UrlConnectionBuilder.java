package org.dandoy.dbpop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class UrlConnectionBuilder implements ConnectionBuilder {
    private final String url;
    private final String username;
    private final String password;

    UrlConnectionBuilder(String url, String username, String password) {
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
