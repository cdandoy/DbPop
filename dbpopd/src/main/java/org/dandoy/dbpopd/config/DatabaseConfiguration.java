package org.dandoy.dbpopd.config;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.UrlConnectionBuilder;

import java.util.Objects;

@Slf4j
public record DatabaseConfiguration(String url, String username, String password, boolean fromEnvVariables) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DatabaseConfiguration that)) return false;

        if (fromEnvVariables != that.fromEnvVariables) return false;
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
        result = 31 * result + (fromEnvVariables ? 1 : 0);
        return result;
    }

    boolean hasInfo() {return url() != null;}

    public ConnectionBuilder createConnectionBuilder() {
        return new UrlConnectionBuilder(url(), username(), password());
    }
}
