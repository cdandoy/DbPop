package org.dandoy.dbpop.download;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.upload.DefaultBuilder;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.Base64;

@Slf4j
public class Downloader implements AutoCloseable {
    /**
     * Maximum length for an individual value
     */
    private static final int MAX_LENGTH = 1024 * 32;
    private final File directory;
    private final Connection connection;
    private final Statement statement;
    private final Database database;

    private Downloader(Connection connection, File directory) throws SQLException {
        this.connection = connection;
        this.statement = connection.createStatement();
        this.directory = directory;
        this.database = Database.createDatabase(connection);
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

    public Database getDatabase() {
        return database;
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
                    if (database.isBinary(metaData, i)) {
                        columnName += "*b64";
                    }
                    csvPrinter.print(columnName);
                }
                csvPrinter.println();

                // Data - it feels like the download*() methods would be better handled by the Database class
                while (resultSet.next()) {
                    for (int i = 0; i < columnCount; i++) {
                        if (metaData.getColumnType(i + 1) == Types.CLOB) {
                            downloadClob(tableName, resultSet, csvPrinter, metaData, i);
                        } else if (metaData.getColumnType(i + 1) == Types.BLOB) {
                            downloadBlob(tableName, resultSet, csvPrinter, metaData, i);
                        } else if (database.isBinary(metaData, i)) {
                            downloadBinary(tableName, resultSet, csvPrinter, metaData, i);
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
                log.error("Data too large: {}.{} - {}Kb",
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
        Blob blob = resultSet.getBlob(i + 1);
        if (blob != null) {
            Base64.Encoder encoder = Base64.getEncoder();
            long length = blob.length();
            if (length <= MAX_LENGTH) {
                byte[] bytes = blob.getBytes(0, (int) length);
                String s = encoder.encodeToString(bytes);
                csvPrinter.print(s);
            } else {
                downloadTooLarge(tableName, csvPrinter, metaData, i, length);
            }
        } else {
            csvPrinter.print(null);
        }
    }

    private void downloadBinary(TableName tableName, ResultSet resultSet, CSVPrinter csvPrinter, ResultSetMetaData metaData, int i) throws SQLException, IOException {
        byte[] bytes = resultSet.getBytes(i + 1);
        if (bytes != null) {
            int length = bytes.length;
            if (length <= MAX_LENGTH) {
                Base64.Encoder encoder = Base64.getEncoder();
                String s = encoder.encodeToString(bytes);
                csvPrinter.print(s);
            } else {
                downloadTooLarge(tableName, csvPrinter, metaData, i, length);
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
                downloadTooLarge(tableName, csvPrinter, metaData, i, length);
            }
        } else {
            csvPrinter.print(null);
        }
    }

    private void downloadTooLarge(TableName tableName, CSVPrinter csvPrinter, ResultSetMetaData metaData, int i, long length) throws IOException, SQLException {
        String columnName = metaData.getColumnName(i + 1);
        log.error("Data too large: {}.{} - {}Kb",
                tableName.toQualifiedName(),
                columnName,
                length / 1024
        );
        csvPrinter.print(null);
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

    public static class Builder extends DefaultBuilder<Builder, Downloader> {
        private File directory;
        private String dataset;

        @Override
        public Downloader build() {
            return Downloader.build(this);
        }

        public File getDirectory() {
            if (directory != null) return directory;
            return findDirectory();
        }

        /**
         * Tries to find the dataset directory starting from the current directory.
         */
        private static File findDirectory() {
            for (File dir = new File("."); dir != null; dir = dir.getParentFile()) {
                File datasetsDirectory = new File(dir, "src/test/resources/datasets");
                if (datasetsDirectory.isDirectory()) {
                    try {
                        return datasetsDirectory.getAbsoluteFile().getCanonicalFile();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            throw new RuntimeException("Datasets directory not set");
        }

        /**
         * For example:
         * <pre>
         * directory/
         *   base/
         *     AdventureWorks/
         *       HumanResources/
         *         Department.csv
         *         Employee.csv
         *         Shift.csv
         * </pre>
         *
         * @param directory The directory that holds the datasets
         * @return this
         */
        public Builder setDirectory(File directory) {
            try {
                if (directory != null) {
                    this.directory = directory.getAbsoluteFile().getCanonicalFile();
                    if (!this.directory.isDirectory()) throw new RuntimeException("Invalid dataset directory: " + this.directory);
                } else {
                    this.directory = null;
                }
                return this;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public Builder setDataset(String dataset) {
            this.dataset = dataset;
            return this;
        }
    }
}
