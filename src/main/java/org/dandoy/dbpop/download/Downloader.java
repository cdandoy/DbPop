package org.dandoy.dbpop.download;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.dandoy.dbpop.Database;
import org.dandoy.dbpop.DefaultBuilder;
import org.dandoy.dbpop.FeatureFlags;
import org.dandoy.dbpop.TableName;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.Base64;
import java.util.regex.Pattern;

public class Downloader implements AutoCloseable {
    /**
     * Maximum length for an individual value
     */
    private static final int MAX_LENGTH = 1024 * 32;
    private final File directory;
    private final Connection connection;
    private final Pattern SPLIT_PATTERN = Pattern.compile("\\.");
    private final Statement statement;
    private final Database database;

    private Downloader(Connection connection, File directory) throws SQLException {
        this.connection = connection;
        this.statement = connection.createStatement();
        this.directory = directory;
        database = Database.createDatabase(connection);
    }

    @Override
    public void close() {
        try {
            database.close();
            statement.close();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    private static Downloader build(Builder builder) {
        try {
            return new Downloader(
                    builder.getConnectionBuilder().createConnection(),
                    new File(
                            builder.getDirectory(),
                            builder.dataset
                    )
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public void download(String table) {
        TableName tableName = getTableName(table);
        download(tableName);
    }

    public void download(TableName tableName) {
        String quotedTableName = database.quote(tableName);
        try (ResultSet resultSet = statement.executeQuery("SELECT * FROM " + quotedTableName)) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .build();
            try (CSVPrinter csvPrinter = new CSVPrinter(getFileWriter(tableName), csvFormat)) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Headers
                for (int i = 0; i < columnCount; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    csvPrinter.print(columnName);
                }
                csvPrinter.println();

                // Data
                while (resultSet.next()) {
                    for (int i = 0; i < columnCount; i++) {
                        if (metaData.getColumnType(i + 1) == Types.CLOB) {
                            downloadClob(tableName, resultSet, csvPrinter, metaData, i);
                        } else if (metaData.getColumnType(i + 1) == Types.BLOB) {
                            downloadBlob(tableName, resultSet, csvPrinter, metaData, i);
                        } else {
                            downloadString(tableName, resultSet, csvPrinter, metaData, i);
                        }
                    }
                    csvPrinter.println();
                }
                csvPrinter.printRecords(resultSet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void downloadClob(TableName tableName, ResultSet resultSet, CSVPrinter csvPrinter, ResultSetMetaData metaData, int i) throws SQLException, IOException {
        Clob clob = resultSet.getClob(i + 1);
        if (clob != null) {
            long length = clob.length();
            if (length <= MAX_LENGTH) {
                try (Reader characterStream = clob.getCharacterStream()) {
                    csvPrinter.print(characterStream);
                }
            } else {
                System.out.printf("Data too large: %s.%s - %dKb%n",
                        tableName.toQualifiedName(),
                        metaData.getColumnName(i + 1),
                        length / 1024
                );
                csvPrinter.print(null);
            }
        } else {
            csvPrinter.print(null);
        }
    }

    private void downloadBlob(TableName tableName, ResultSet resultSet, CSVPrinter csvPrinter, ResultSetMetaData metaData, int i) throws SQLException, IOException {
        if (FeatureFlags.HANDLE_BINARY) {
            Blob blob = resultSet.getBlob(i + 1);
            if (blob != null) {
                Base64.Encoder encoder = Base64.getEncoder();
                long length = blob.length();
                if (length <= MAX_LENGTH) {
                    byte[] bytes = blob.getBytes(0, (int) length);
                    String s = encoder.encodeToString(bytes);
                    csvPrinter.print(s);
                } else {
                    System.out.printf("Data too large: %s.%s - %dKb%n",
                            tableName.toQualifiedName(),
                            metaData.getColumnName(i + 1),
                            length / 1024
                    );
                    csvPrinter.print(null);
                }
            } else {
                csvPrinter.print(null);
            }
        } else {
            csvPrinter.print(null);
        }
    }

    private void downloadString(TableName tableName, ResultSet resultSet, CSVPrinter csvPrinter, ResultSetMetaData metaData, int i) throws SQLException, IOException {
        String s = resultSet.getString(i + 1);
        if (s != null) {
            int length = s.length();
            if (length <= MAX_LENGTH) {
                csvPrinter.print(s);
            } else {
                System.out.printf("Data too large: %s.%s - %dKb%n",
                        tableName.toQualifiedName(),
                        metaData.getColumnName(i + 1),
                        length / 1024
                );
                csvPrinter.print(null);
            }
        } else {
            csvPrinter.print(null);
        }
    }

    private Writer getFileWriter(TableName tableName) {
        File catalogDir = new File(directory, tableName.getCatalog());
        File schemaDir = new File(catalogDir, tableName.getSchema());
        if (!schemaDir.mkdirs()) {
            if (!schemaDir.isDirectory()) {
                throw new RuntimeException("Cannot create the directory " + schemaDir);
            }
        }
        File file = new File(schemaDir, tableName.getTable() + ".csv");
        try {
            return Files.newBufferedWriter(file.toPath(), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create ", e);
        }
    }

    private TableName getTableName(String table) {
        try {
            String[] split = SPLIT_PATTERN.split(table);
            if (split.length == 1) {
                return new TableName(connection.getCatalog(), connection.getSchema(), split[0]);
            } else if (split.length == 2) {
                return new TableName(connection.getCatalog(), split[0], split[1]);
            } else if (split.length == 3) {
                return new TableName(split[0], split[1], split[2]);
            } else {
                throw new RuntimeException("Invalid table name: " + table);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Builder extends DefaultBuilder<Builder, Downloader> {
        private String dataset;

        @Override
        public Downloader build() {
            super.validate();
            return Downloader.build(this);
        }

        public Builder setDataset(String dataset) {
            this.dataset = dataset;
            return this;
        }
    }
}
