package org.dandoy.dbpopd.config;

import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.UrlConnectionBuilder;

public record DatabaseConfiguration(String url, String username, String password, boolean conflict) {
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

    boolean hasInfo() {return url() != null;}

    public ConnectionBuilder createConnectionBuilder() {
        return hasInfo() ? new UrlConnectionBuilder(url(), username(), password()) : null;
    }
}
