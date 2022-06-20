package org.dandoy.dbpop;

import java.sql.Connection;
import java.sql.SQLException;

interface ConnectionBuilder {
    Connection createConnection() throws SQLException;
}
