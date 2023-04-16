package org.dandoy.dbpop.database;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class UrlConnectionBuilder implements ConnectionBuilder {
    public static final int WAIT_COUNT = 300;    // Wait N times for a connection
    public static final int WAIT_TIME = 1000;   // Wait N millis between each retry
    @Getter
    private final String url;
    @Getter
    private final String username;
    @Getter
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
     * Tests at the TCP/IP level that we can connect to the port
     */
    @Override
    public void testConnection() {
        Pattern pattern = Pattern.compile("jdbc:sqlserver://(\\w+)(:(\\d+))?(;.*)");
        Matcher matcher = pattern.matcher(url);
        if (!matcher.matches()) return;

        String host = matcher.group(1);
        String portGroup = matcher.group(3);
        int port = 1433;
        if (portGroup != null) {
            port = Integer.parseInt(portGroup);
        }
        log.info("Waiting for the database to respond");
        for (int i = 0; i < WAIT_COUNT; i++) {
            try {
                try (Socket socket = new Socket(host, port)) {
                    try (InputStream ignored = socket.getInputStream()) {
                        return;
                    }
                }
            } catch (Exception ignored) {
            }
            try {
                Thread.sleep(WAIT_TIME);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("Failed to connect to the database socket");
    }

    /**
     * When running in a Docker environment, wait for the database to be started
     */
    private Connection waitForConnection() throws SQLException {
        Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerConnection");
        try {
            logger.setLevel(Level.OFF);
            if (!hasWaited) {
                DriverManager.setLoginTimeout(5);
                log.info("Waiting for the database to connect");
                // Try several times, wait between each
                for (int i = 0; i < WAIT_COUNT; i++) {
                    try {
                        return DriverManager.getConnection(url, username, password);
                    } catch (SQLException e) {
                        try {
                            Thread.sleep(WAIT_TIME); // wait then retry
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            }

            // Try one more time, log the error
            try {
                return DriverManager.getConnection(url, username, password);
            } catch (SQLException e) {
                log.error("Failed to connect to " + url, e);
                throw e;
            }
        } finally {
            logger.setLevel(Level.INFO);
            hasWaited = true;
        }
    }
}
