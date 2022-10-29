package org.dandoy.dbpop.utils;

import java.sql.Connection;
import java.sql.SQLException;

public class AutoComitterOff implements AutoCloseable {
    private final boolean autoCommit;
    private final Connection connection;

    public AutoComitterOff(Connection connection) {
        this.connection = connection;
        try {
            autoCommit = connection.getAutoCommit();
            if (autoCommit) {
                connection.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (autoCommit) {
            try {
                connection.commit();
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
