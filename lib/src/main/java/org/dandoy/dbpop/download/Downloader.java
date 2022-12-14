package org.dandoy.dbpop.download;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpop.utils.DbPopUtils;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.dandoy.dbpop.datasets.Datasets.BASE;
import static org.dandoy.dbpop.datasets.Datasets.STATIC;

@Slf4j
public class Downloader implements AutoCloseable {
    /**
     * Maximum length for an individual value
     */
    private static final int MAX_LENGTH = 1024 * 32;
    private static final Set<Class<?>> VALID_WHERE_CLASSES = new HashSet<>(Arrays.asList(
            String.class,
            Float.class,
            BigDecimal.class,
            Long.class,
            Double.class,
            Short.class,
            BigInteger.class,
            Byte.class,
            Integer.class
    ));
    private final File directory;
    private final Database database;
    private final PkReader pkReader;

    private Downloader(Database database, File directory, PkReader pkReader) throws SQLException {
        this.directory = directory;
        this.database = database;
        this.pkReader = pkReader;
    }

    @Override
    public void close() {
        database.close();
    }

    public Connection getConnection() {
        return database.getConnection();
    }

    public Database getDatabase() {
        return database;
    }

    private static Downloader build(Builder builder) {
        try {
            if (builder.getDbUrl() == null) throw new RuntimeException("Missing --jdbcurl");
            if (builder.dataset == null) throw new RuntimeException("Missing dataset");

            Connection connection = builder.getConnectionBuilder().createConnection();
            Database database = Database.createDatabase(connection);
            PkReader pkReader = createPkReader(database, builder.directory, builder.dataset);
            return new Downloader(
                    database,
                    new File(
                            builder.getDirectory(),
                            builder.dataset
                    ),
                    pkReader
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static PkReader createPkReader(
            Database database,
            File datasetsDirectory,
            String loadedDataset
    ) {
        List<Dataset> pkDatasets = Datasets.getDatasets(datasetsDirectory)
                .stream()
                .filter(dataset -> STATIC.equals(dataset.getName()) || BASE.equals(dataset.getName()) || loadedDataset.equals(dataset.getName()))
                .collect(Collectors.toList());
        return PkReader.readDatasets(database, pkDatasets);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void download(TableName tableName) {
        download(tableName, Collections.emptyList());
    }

    public void download(TableName tableName, List<Where> wheres) {
        List<ColumnWhere> columnWheres = getColumnWheres(tableName, wheres);
        String selectStatement = createSelectStatement(tableName, columnWheres);
        try (PreparedStatement preparedStatement = database.getConnection().prepareStatement(selectStatement)) {
            // Bind the where clauses
            for (int i = 0; i < columnWheres.size(); i++) {
                ColumnWhere columnWhere = columnWheres.get(i);
                ColumnType columnType = columnWhere.getColumn().getColumnType();
                Object value = columnWhere.getValue();
                columnType.bind(preparedStatement, i + 1, value);
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                List<String> metaDataHeaders = getHeaders(metaData);
                CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                        .build();

                File file = getFile(tableName);
                boolean append = file.isFile();
                if (append) {
                    compareHeaders(file, metaDataHeaders);
                }

                try (CSVPrinter csvPrinter = new CSVPrinter(Files.newBufferedWriter(file.toPath(), UTF_8, append ? APPEND : CREATE_NEW), csvFormat)) {

                    if (!append) {
                        csvPrinter.printRecord(metaDataHeaders);
                    }

                    // PKs
                    PkReader.PkInfo pkInfo = pkReader.getPkInfo(tableName);
                    List<Integer> pkPositions = pkInfo.toPositions(metaData);

                    // Data - it feels like the download*() methods would be better handled by the Database class
                    List<String> pk = new ArrayList<>();
                    while (resultSet.next()) {
                        pk.clear();
                        for (Integer pos : pkPositions) {
                            pk.add(getString(resultSet, pos));
                        }
                        if (pkInfo.containsRow(pk)) continue;

                        int columnCount = metaData.getColumnCount();
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
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getHeaders(ResultSetMetaData metaData) throws SQLException {
        List<String> ret = new ArrayList<>();
        int columnCount = metaData.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            String columnName = metaData.getColumnName(i + 1);
            if (database.isBinary(metaData, i)) {
                columnName += "*b64";
            }
            ret.add(columnName);
        }
        return ret;
    }

    private void compareHeaders(File file, List<String> metaDataHeaders) {
        try (CSVParser csvParser = DbPopUtils.createCsvParser(file)) {
            List<String> fileHeaders = csvParser.getHeaderNames();
            if (!fileHeaders.equals(metaDataHeaders)) {
                throw new RuntimeException(String.format(
                        "Headers of %s do not match the database:\n  Database: %s\n  File    : %s\n",
                        file,
                        metaDataHeaders,
                        fileHeaders
                ));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read the headers of " + file, e);
        }
    }

    private List<ColumnWhere> getColumnWheres(TableName tableName, List<Where> wheres) {
        Collection<Table> tables = database.getTables(Collections.singleton(tableName));
        if (tables.isEmpty()) throw new RuntimeException("Table does not exist: " + tableName.toQualifiedName());
        Map<String, Column> columnMap = tables.iterator().next().getColumns().stream().
                collect(Collectors.toMap(
                        Column::getName,
                        Function.identity()
                ));

        return wheres.stream()
                .map(where -> {
                    // Validate that the column names are in the table
                    String columnName = where.getColumn();
                    Column tableColumn = columnMap.get(columnName);
                    if (tableColumn == null) throw new RuntimeException(String.format(
                            "Column does not exist: %s in table %s",
                            columnName, tableName.toQualifiedName()
                    ));

                    // Validate that the values are either strings or numbers
                    Object value = where.getValue();
                    if (value != null && !VALID_WHERE_CLASSES.contains(value.getClass())) {
                        throw new RuntimeException("WHERE values must be strings or numbers");
                    }

                    return new ColumnWhere(tableColumn, value);
                })
                .collect(Collectors.toList());
    }

    private String createSelectStatement(TableName tableName, List<ColumnWhere> wheres) {
        String quotedTableName = database.quote(tableName);
        String ret = "SELECT *\nFROM " + quotedTableName;

        if (!wheres.isEmpty()) {
            String whereClauses = wheres.stream()
                    .map(where -> {
                        String columnName = where.getColumn().getName();
                        String quotedColumnName = database.quote(columnName);
                        return String.format("%s = ?", quotedColumnName);
                    })
                    .collect(Collectors.joining("\nAND "));
            ret += "\nWHERE " + whereClauses;
        }
        return ret;
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

    private String getString(ResultSet resultSet, int i) {
        try {
            return resultSet.getString(i + 1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
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

    private File getFile(TableName tableName) {
        File dir = directory;
        if (tableName.getCatalog() != null) dir = new File(dir, tableName.getCatalog());
        if (tableName.getSchema() != null) dir = new File(dir, tableName.getSchema());

        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new RuntimeException("Cannot create the directory " + dir);
        }
        return new File(dir, tableName.getTable() + ".csv");
    }

    @Getter
    @Setter
    @Accessors(chain = true)
    public static class Builder {
        private String dbUrl;
        private String dbUser;
        private String dbPassword;
        private File directory;
        private String dataset;

        public Downloader build() {
            return Downloader.build(this);
        }

        public ConnectionBuilder getConnectionBuilder() {
            return new UrlConnectionBuilder(
                    getDbUrl(),
                    getDbUser(),
                    getDbPassword()
            );
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
                File datasetsDirectory = new File(dir, "src/test/resources/");
                if (datasetsDirectory.isDirectory()) {
                    try {
                        File testdata = new File(datasetsDirectory, "testdata");
                        if (testdata.isDirectory() || testdata.mkdirs()) {
                            return testdata.getAbsoluteFile().getCanonicalFile();
                        }
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

    @Getter
    @AllArgsConstructor
    private static class ColumnWhere {
        private final Column column;
        private final Object value;
    }
}
