package org.dandoy.dbpopd.config;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.UrlConnectionBuilder;

import java.io.InputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public record DatabaseConfiguration(String url, String username, String password, boolean conflict) {
    private static final Pattern JDBC_URL_PARSER = Pattern.compile("jdbc:sqlserver://(\\w+)(:(\\d+))?(;.*)");

    public DatabaseConfiguration(String url, String username, String password) {
        this(url, username, password, false);
    }

    public DatabaseConfiguration(DatabaseConfiguration that, boolean conflict) {
        this(
                that.url(),
                that.username(),
                that.password(),
                conflict
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DatabaseConfiguration that)) return false;

        if (conflict != that.conflict) return false;
        if (!Objects.equals(url, that.url)) return false;
        if (!Objects.equals(username, that.username)) return false;
        if (!Objects.equals(password, that.password)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = url != null ? url.hashCode() : 0;
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (conflict ? 1 : 0);
        return result;
    }

    boolean hasInfo() {return url() != null;}

    public ConnectionBuilder createConnectionBuilder() {
        if (hasInfo()) {
            if (canTcpConnect()) {
                UrlConnectionBuilder urlConnectionBuilder = new UrlConnectionBuilder(url(), username(), password());
                try (Connection ignored = urlConnectionBuilder.createConnection()) {
                    return urlConnectionBuilder;
                } catch (SQLException e) {
                    log.error("Failed to connect to {} with username {}", url(), username());
                    return null;
                }
            }
        }
        return null;
    }

    private boolean canTcpConnect() {
        Matcher matcher = JDBC_URL_PARSER.matcher(url);
        if (!matcher.matches()) {
            log.warn("Could not parse the connection string '{}'", url);
            return true; // Assume we can
        }

        String host = matcher.group(1);
        String portGroup = matcher.group(3);
        int port = 1433;
        if (portGroup != null) {
            port = Integer.parseInt(portGroup);
        }
        log.debug("Attempting a socket connection to {}:{}", host, port);
        try (Socket socket = new Socket(host, port)) {
            try (InputStream ignored = socket.getInputStream()) {
                return true;
            }
        } catch (Exception ignore) {
            return false;
        }
    }
}
