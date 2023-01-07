package org.dandoy.dbpop.database;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class UrlConnectionBuilder implements ConnectionBuilder {
    public static final int WAIT_COUNT = 5;    // Wait N times for a connection
    public static final int WAIT_TIME = 3000;   // Wait N millis between each retry
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
        for (int i = 0; i < WAIT_COUNT; i++) {  // Try 20 times
            try {
                return DriverManager.getConnection(url, username, password);
            } catch (SQLException e) {
                if (hasWaited) throw e;
                lastException = e;
                log.info("Waiting for SQL Server");
                try {
                    Thread.sleep(WAIT_TIME); // wait then retry
                } catch (InterruptedException ignored) {
                }
            }
        }
        hasWaited = true;

        throw lastException;
    }
}
