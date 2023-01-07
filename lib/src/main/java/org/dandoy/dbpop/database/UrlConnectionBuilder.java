package org.dandoy.dbpop.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class UrlConnectionBuilder implements ConnectionBuilder {
    private final String url;
    private final String username;
    private final String password;
    private boolean hasWaited;

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
        return waitForConnection();
    }

    /**
     * When running in a Docker environment, wait for the database to be started
     */
    private Connection waitForConnection() throws SQLException {
        SQLException lastException = null;
        for (int i = 0; i < 20; i++) {  // Try 20 times
            try {
                return DriverManager.getConnection(url, username, password);
            } catch (SQLException e) {
                if (hasWaited) throw e;
                lastException = e;
                try {
                    Thread.sleep(1000); // wait 1 second then retry
                } catch (InterruptedException ignored) {
                }
            }
        }
        hasWaited = true;

        throw lastException;
    }
}
