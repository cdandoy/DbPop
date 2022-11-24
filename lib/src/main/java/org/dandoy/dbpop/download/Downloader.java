package org.dandoy.dbpop.download;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpop.upload.DefaultBuilder;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

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
            @SuppressWarnings("unused") String loadedDataset // We don't include the loaded dataset yet because the downloader overrides the file, it doesn't append. See #15
    ) {
        List<Dataset> pkDatasets = Datasets.getDatasets(datasetsDirectory)
                .stream()
                .filter(dataset -> STATIC.equals(dataset.getName()) || BASE.equals(dataset.getName())/* || loadedDataset.equals(dataset.getName())*/)
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
