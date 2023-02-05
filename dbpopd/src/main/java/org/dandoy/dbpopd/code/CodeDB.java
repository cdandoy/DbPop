package org.dandoy.dbpopd.code;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles the interaction with the dbpop_timestamps table.
 */
@Singleton
@Slf4j
class CodeDB {
    /**
     * Creates the object to insert/update rows in dbpop_timestamps
     */
    static TimestampInserter createTimestampInserter(Database database) throws SQLException {
        checkDbpopTable(database);

        Connection connection = database.getConnection();
        Map<TimestampObject, Timestamp> timestamps = getObjectTimestampMap(connection);
        return new TimestampInserter(timestamps, connection);
    }

    static Map<TimestampObject, Timestamp> getObjectTimestampMap(Connection connection) {
        Map<TimestampObject, Timestamp> timestamps = new HashMap<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT object_type, object_catalog, object_schema, object_name, created FROM master.dbo.dbpop_timestamps")) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    TimestampObject timestampObject = TimestampInserter.toTimestampObject(resultSet);
                    Timestamp created = resultSet.getTimestamp("created");
                    timestamps.put(timestampObject, created);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return timestamps;
    }

    /**
     * Checks if the table dbpop_timestamps exists, creates it if it doesn't
     */
    private static void checkDbpopTable(Database database) throws SQLException {
        Table table = database.getTable(new TableName("master", "dbo", "dbpop_timestamps"));
        if (table == null) {
            createTimestampTable(database);
        }
    }

    /**
     * Creates the dbpop_timestamps table
     */
    private static void createTimestampTable(Database database) throws SQLException {
        try (PreparedStatement preparedStatement = database.getConnection().prepareStatement("""
                CREATE TABLE master.dbo.dbpop_timestamps
                (
                    object_type    VARCHAR(64),
                    object_catalog VARCHAR(256),
                    object_schema  VARCHAR(256),
                    object_name    VARCHAR(256),
                    created        DATETIME,
                    CONSTRAINT dbpop_timestamps_pk PRIMARY KEY (object_type, object_catalog, object_schema, object_name)
                )""")) {
            preparedStatement.execute();
        }
    }

    static class TimestampInserter implements AutoCloseable {
        private final Map<TimestampObject, Timestamp> timestamps;
        private final PreparedStatement insertStatement;
        private final PreparedStatement updateStatement;

        public TimestampInserter(Map<TimestampObject, Timestamp> timestamps, Connection connection) throws SQLException {
            this.timestamps = timestamps;
            insertStatement = connection.prepareStatement("INSERT INTO master.dbo.dbpop_timestamps (object_type, object_catalog, object_schema, object_name, created) VALUES (?, ?, ?, ?, ?)");
            updateStatement = connection.prepareStatement("""
                    UPDATE master.dbo.dbpop_timestamps
                    SET created=?
                    WHERE object_type = ?
                      AND object_catalog = ?
                      AND object_schema = ?
                      AND object_name = ?
                    """);
        }

        @Override
        public void close() throws SQLException {
            flush();
            insertStatement.close();
            updateStatement.close();
        }

        private void flush() throws SQLException {
            insertStatement.executeBatch();
            updateStatement.executeBatch();
        }

        void addTimestamp(String type, String catalog, String schema, String name, Timestamp created) throws SQLException {
            TimestampObject timestampObject = new TimestampObject(type, catalog, schema, name);
            Timestamp timestamp = timestamps.put(timestampObject, created);
            if (timestamp == null) {
                insertStatement.setString(1, type);
                insertStatement.setString(2, catalog);
                insertStatement.setString(3, schema);
                insertStatement.setString(4, name);
                insertStatement.setTimestamp(5, created);
                insertStatement.addBatch();
            } else if (!timestamp.equals(created)) {
                updateStatement.setTimestamp(1, created);
                updateStatement.setString(2, type);
                updateStatement.setString(3, catalog);
                updateStatement.setString(4, schema);
                updateStatement.setString(5, name);
                updateStatement.addBatch();
            }
        }

        static TimestampObject toTimestampObject(ResultSet resultSet) throws SQLException {
            String type = resultSet.getString("object_type");
            String catalog = resultSet.getString("object_catalog");
            String schema = resultSet.getString("object_schema");
            String name = resultSet.getString("object_name");
            return new TimestampObject(type, catalog, schema, name);
        }
    }

    record TimestampObject(String type, String catalog, String schema, String name) {}
}
