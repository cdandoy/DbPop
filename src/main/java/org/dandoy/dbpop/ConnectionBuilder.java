package org.dandoy.dbpop;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionBuilder {
    Connection createConnection() throws SQLException;
}
