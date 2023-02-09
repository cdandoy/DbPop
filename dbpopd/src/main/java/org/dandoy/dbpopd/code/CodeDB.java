package org.dandoy.dbpopd.code;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;
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
        Map<ObjectIdentifier, CodeTimestamps> timestamps = getObjectTimestampMap(connection);
        return new TimestampInserter(timestamps, connection);
    }

    static Map<ObjectIdentifier, CodeTimestamps> getObjectTimestampMap(Connection connection) {
        Map<ObjectIdentifier, CodeTimestamps> timestamps = new HashMap<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT object_type, object_catalog, object_schema, object_name, file_timestamp, code_timestamp FROM master.dbo.dbpop_timestamps")) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    ObjectIdentifier objectIdentifier = TimestampInserter.toTimestampObject(resultSet);
                    Timestamp fileTimestamp = resultSet.getTimestamp("file_timestamp");
                    Timestamp codeTimestamp = resultSet.getTimestamp("code_timestamp");
                    timestamps.put(objectIdentifier, new CodeTimestamps(fileTimestamp, codeTimestamp));
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
        // SQL Server's DATETIME rounds the millis up to .007 seconds.
        // https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql#:~:text=datetime%20values%20are%20rounded%20to,shown%20in%20the%20following%20table.
        String dateTimeType = database.isSqlServer() ? "DATETIME2" : "DATETIME";
        try (PreparedStatement preparedStatement = database.getConnection().prepareStatement("""
                CREATE TABLE master.dbo.dbpop_timestamps
                (
                    object_type    VARCHAR(64),
                    object_catalog VARCHAR(256),
                    object_schema  VARCHAR(256),
                    object_name    VARCHAR(256),
                    file_timestamp %s,
                    code_timestamp %s,
                    CONSTRAINT dbpop_timestamps_pk PRIMARY KEY (object_type, object_catalog, object_schema, object_name)
                )""".formatted(dateTimeType, dateTimeType))) {
            preparedStatement.execute();
        }
    }

    static class TimestampInserter implements AutoCloseable {
        private final Map<ObjectIdentifier, CodeTimestamps> timestamps;
        private final PreparedStatement insertStatement;
        private final PreparedStatement updateStatement;

        public TimestampInserter(Map<ObjectIdentifier, CodeTimestamps> timestamps, Connection connection) throws SQLException {
            this.timestamps = timestamps;
            insertStatement = connection.prepareStatement("INSERT INTO master.dbo.dbpop_timestamps (object_type, object_catalog, object_schema, object_name, file_timestamp, code_timestamp) VALUES (?, ?, ?, ?, ?, ?)");
            updateStatement = connection.prepareStatement("""
                    UPDATE master.dbo.dbpop_timestamps
                    SET file_timestamp = ?, code_timestamp = ?
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

        public CodeTimestamps getTimestamp(ObjectIdentifier objectIdentifier) {
            return timestamps.get(objectIdentifier);
        }

        void addTimestamp(ObjectIdentifier objectIdentifier, Timestamp fileTimestamp, Timestamp codeTimestamp) throws SQLException {
            CodeTimestamps newCodeTimestamps = new CodeTimestamps(fileTimestamp, codeTimestamp);
            CodeTimestamps oldCodeTimestamps = this.timestamps.put(objectIdentifier, newCodeTimestamps);
            if (oldCodeTimestamps == null) {
                insertStatement.setString(1, objectIdentifier.getType());
                insertStatement.setString(2, objectIdentifier.getCatalog());
                insertStatement.setString(3, objectIdentifier.getSchema());
                insertStatement.setString(4, objectIdentifier.getName());
                insertStatement.setTimestamp(5, fileTimestamp);
                insertStatement.setTimestamp(6, codeTimestamp);
                insertStatement.addBatch();
            } else if (!oldCodeTimestamps.equals(newCodeTimestamps)) {
                updateStatement.setTimestamp(1, fileTimestamp);
                updateStatement.setTimestamp(2, codeTimestamp);
                updateStatement.setString(3, objectIdentifier.getType());
                updateStatement.setString(4, objectIdentifier.getCatalog());
                updateStatement.setString(5, objectIdentifier.getSchema());
                updateStatement.setString(6, objectIdentifier.getName());
                updateStatement.addBatch();
            }
        }

        static ObjectIdentifier toTimestampObject(ResultSet resultSet) throws SQLException {
            String type = resultSet.getString("object_type");
            String catalog = resultSet.getString("object_catalog");
            String schema = resultSet.getString("object_schema");
            String name = resultSet.getString("object_name");
            return new ObjectIdentifier(type, catalog, schema, name);
        }
    }
}
