package org.dandoy.dbpop.download;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ColumnType;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.datasets.Datasets;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

@Slf4j
public class TableDownloader implements AutoCloseable {
    private static final int MAX_LENGTH = 1024 * 32;
    private final TableName tableName;
    private final TablePrimaryKeys tablePrimaryKeys;
    private final TableFetcher tableFetcher;
    private final DeferredCsvPrinter csvPrinter;
    private final ExecutionContext executionContext;
    private final List<SelectedColumn> selectedColumns;
    private final List<Consumer<ResultSet>> consumers = new ArrayList<>();

    private TableDownloader(TableName tableName, TablePrimaryKeys tablePrimaryKeys, TableFetcher tableFetcher, List<SelectedColumn> selectedColumns, @Nullable DeferredCsvPrinter csvPrinter, ExecutionContext executionContext) {
        this.tableName = tableName;
        this.tablePrimaryKeys = tablePrimaryKeys;
        this.tableFetcher = tableFetcher;
        this.selectedColumns = selectedColumns;
        this.csvPrinter = csvPrinter;
        this.executionContext = executionContext;
        this.consumers.add(this::consumeResultSet);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static TableDownloader createTableDownloader(Database database, File datasetsDirectory, String dataset,
                                                         TableName tableName, List<TableJoin> tableJoins, List<TableQuery> where,
                                                         List<String> filteredColumns, boolean forceEmpty,
                                                         ExecutionMode executionMode, ExecutionContext executionContext) {
        Table table = database.getTable(tableName);
        TableFetcher tableFetcher = TableFetcher.createTableFetcher(database, table, tableJoins, where, filteredColumns, executionContext);
        List<SelectedColumn> selectedColumns = tableFetcher.getSelectedColumns();
        TablePrimaryKeys tablePrimaryKeys = TablePrimaryKeys.createTablePrimaryKeys(datasetsDirectory, dataset, table, selectedColumns);

        OutputFile outputFile = OutputFile.createOutputFile(datasetsDirectory, dataset, tableName, forceEmpty);
        outputFile.setColumns(selectedColumns);
        if (!outputFile.isNewFile()) {
            // If the file exists, we must only fetch the columns found in the headers
            selectedColumns = filterSelectedColumns(selectedColumns, outputFile.getHeaders());
        }

        DeferredCsvPrinter csvPrinter = null;
        if (executionMode == ExecutionMode.SAVE) {
            csvPrinter = outputFile.createCsvPrinter();
        }

        return new TableDownloader(tableName, tablePrimaryKeys, tableFetcher, selectedColumns, csvPrinter, executionContext);
    }

    private static List<SelectedColumn> filterSelectedColumns(List<SelectedColumn> selectedColumns, List<String> headers) {
        List<SelectedColumn> ret = new ArrayList<>();
        for (String header : headers) {
            SelectedColumn found = null;
            for (SelectedColumn selectedColumn : selectedColumns) {
                if (header.equals(selectedColumn.asHeaderName())) {
                    found = selectedColumn;
                    break;
                }
            }
            ret.add(found);
        }
        return ret;
    }

    @Override
    public void close() {
        if (csvPrinter != null) csvPrinter.close();
        tableFetcher.close();
    }

    @Override
    public String toString() {
        return "TableDownloader{" +
               "tableName=" + tableName +
               '}';
    }

    public void addConsumer(Consumer<ResultSet> consumer) {
        consumers.add(consumer);
    }

    public List<SelectedColumn> getSelectedColumns() {
        return tableFetcher.getSelectedColumns();
    }

    /**
     * Download by primary keys
     */
    public void download(Set<List<Object>> pks) {
        tableFetcher.execute(pks, this::dispatchResultSet);
    }

    /**
     * Download the whole table
     */
    public void download() {
        tableFetcher.execute(Collections.emptySet(), this::dispatchResultSet);
    }

    private boolean dispatchResultSet(ResultSet resultSet) {
        for (Consumer<ResultSet> consumer : consumers) {
            consumer.accept(resultSet);
        }
        return executionContext.keepRunning();
    }

    private void consumeResultSet(ResultSet resultSet) {
        if (isExistingPk(resultSet)) {
            executionContext.rowSkipped(tableName);
            return;
        }

        log.debug("{} prints a row", tableName);
        executionContext.rowAdded(tableName);

        if (csvPrinter != null) {
            try {
                for (SelectedColumn selectedColumn : selectedColumns) {
                    if (selectedColumn == null) {
                        csvPrinter.print(null);
                        continue;
                    }
                    ColumnType columnType = selectedColumn.columnType();
                    if (columnType == ColumnType.INVALID) {
                        csvPrinter.print(null);
                        continue;
                    }
                    Integer integer = columnType.toSqlType();
                    String columnName = selectedColumn.name();
                    int jdbcPos = selectedColumn.jdbcPos();
                    if (integer == Types.CLOB) {
                        downloadClob(resultSet, csvPrinter, columnName, jdbcPos);
                    } else if (integer == Types.BLOB) {
                        downloadBlob(resultSet, csvPrinter, columnName, jdbcPos);
                    } else if (selectedColumn.binary()) {
                        downloadBinary(resultSet, csvPrinter, columnName, jdbcPos);
                    } else {
                        downloadString(resultSet, csvPrinter, columnName, jdbcPos);
                    }
                }
                csvPrinter.println();
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Checks if the PK in the ResultSet has already been downloaded
     */
    private boolean isExistingPk(ResultSet resultSet) {
        if (tablePrimaryKeys == null) return false;
        return !tablePrimaryKeys.addPrimaryKey(resultSet);
    }

    private void downloadClob(ResultSet resultSet, DeferredCsvPrinter csvPrinter, String columnName, int jdbcPos) throws SQLException, IOException {
        Clob clob = resultSet.getClob(jdbcPos);
        if (clob != null) {
            long length = clob.length();
            if (length <= MAX_LENGTH) {
                try (Reader characterStream = clob.getCharacterStream()) {
                    csvPrinter.print(characterStream);
                }
            } else {
                log.error("Data too large: {}.{} - {}Kb",
                        tableName.toQualifiedName(),
                        columnName,
                        length / 1024
                );
                csvPrinter.print(null);
            }
        } else {
            csvPrinter.print(null);
        }
    }

    private void downloadBlob(ResultSet resultSet, DeferredCsvPrinter csvPrinter, String columnName, int jdbcPos) throws SQLException, IOException {
        Blob blob = resultSet.getBlob(jdbcPos);
        if (blob != null) {
            Base64.Encoder encoder = Base64.getEncoder();
            long length = blob.length();
            if (length <= MAX_LENGTH) {
                byte[] bytes = blob.getBytes(0, (int) length);
                String s = encoder.encodeToString(bytes);
                csvPrinter.print(s);
            } else {
                downloadTooLarge(csvPrinter, columnName, length);
            }
        } else {
            csvPrinter.print(null);
        }
    }

    private void downloadBinary(ResultSet resultSet, DeferredCsvPrinter csvPrinter, String columnName, int jdbcPos) throws SQLException, IOException {
        byte[] bytes = resultSet.getBytes(jdbcPos);
        if (bytes != null) {
            int length = bytes.length;
            if (length <= MAX_LENGTH) {
                Base64.Encoder encoder = Base64.getEncoder();
                String s = encoder.encodeToString(bytes);
                csvPrinter.print(s);
            } else {
                downloadTooLarge(csvPrinter, columnName, length);
            }
        } else {
            csvPrinter.print(null);
        }
    }

    private void downloadString(ResultSet resultSet, DeferredCsvPrinter csvPrinter, String columnName, int jdbcPos) throws SQLException, IOException {
        String s = resultSet.getString(jdbcPos);
        if (s != null) {
            int length = s.length();
            if (length <= MAX_LENGTH) {
                csvPrinter.print(s);
            } else {
                downloadTooLarge(csvPrinter, columnName, length);
            }
        } else {
            csvPrinter.print(null);
        }
    }

    private void downloadTooLarge(DeferredCsvPrinter csvPrinter, String columnName, long length) throws IOException {
        log.error("Data too large: {}.{} - {}Kb",
                tableName.toQualifiedName(),
                columnName,
                length / 1024
        );
        csvPrinter.print(null);
    }

    @Getter
    @Setter
    @Accessors(chain = true)
    public static class Builder {
        private Database database;
        private File datasetsDirectory;
        private String dataset;
        private TableName tableName;
        private ExecutionMode executionMode = ExecutionMode.SAVE;
        private ExecutionContext executionContext = new ExecutionContext();
        private List<String> filteredColumns = Collections.emptyList();
        private List<TableJoin> tableJoins=Collections.emptyList();
        private List<TableQuery> wheres=Collections.emptyList();

        public TableDownloader build() {
            if (database == null) throw new RuntimeException("database not set");
            if (datasetsDirectory == null) throw new RuntimeException("datasetsDirectory not set");
            if (dataset == null) throw new RuntimeException("dataset not set");
            if (tableName == null) throw new RuntimeException("tableName not set");

            // An CSV file, even empty, will make dbdpop aware of the table.
            // Having an empty CSV file in /static/ or /base/ will make sure the table is deleted
            // However, we do not want to have empty CSV files in other dataset directories
            boolean forceEmpty = dataset.equals(Datasets.STATIC) || dataset.equals(Datasets.BASE);
            return createTableDownloader(database, datasetsDirectory, dataset, tableName, tableJoins, wheres, filteredColumns, forceEmpty, executionMode, executionContext);
        }
    }
}
