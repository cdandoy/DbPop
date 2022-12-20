package org.dandoy.dbpop.download;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;

@Slf4j
public class TableDownloader implements AutoCloseable {
    private static final int MAX_LENGTH = 1024 * 32;
    private final TableName tableName;
    private final OutputFile outputFile;
    private final TablePrimaryKeys tablePrimaryKeys;
    private final TableExecutor tableExecutor;
    private final List<TableExecutor.SelectedColumn> selectedColumns;

    private TableDownloader(TableName tableName, OutputFile outputFile, TablePrimaryKeys tablePrimaryKeys, TableExecutor tableExecutor, List<TableExecutor.SelectedColumn> selectedColumns) {
        this.tableName = tableName;
        this.outputFile = outputFile;
        this.tablePrimaryKeys = tablePrimaryKeys;
        this.tableExecutor = tableExecutor;
        this.selectedColumns = selectedColumns;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static TableDownloader createTableDownloader(Database database, File datasetsDirectory, String dataset, TableName tableName, boolean byPrimaryKey) {
        OutputFile outputFile = OutputFile.createOutputFile(datasetsDirectory, dataset, tableName);
        Table table = database.getTable(tableName);
        TableExecutor tableExecutor = TableExecutor.createTableExecutor(database, table, byPrimaryKey);
        List<TableExecutor.SelectedColumn> selectedColumns = tableExecutor.getSelectedColumns();
        TablePrimaryKeys tablePrimaryKeys = TablePrimaryKeys.createTablePrimaryKeys(datasetsDirectory, dataset, table, selectedColumns);

        if (outputFile.isNewFile()) {
            outputFile.setColumns(selectedColumns);
        } else {
            selectedColumns = filterSelectedColumns(selectedColumns, outputFile.getHeaders());
        }

        return new TableDownloader(tableName, outputFile, tablePrimaryKeys, tableExecutor, selectedColumns);
    }

    private static List<TableExecutor.SelectedColumn> filterSelectedColumns(List<TableExecutor.SelectedColumn> selectedColumns, List<String> headers) {
        List<TableExecutor.SelectedColumn> ret = new ArrayList<>();
        for (String header : headers) {
            TableExecutor.SelectedColumn found = null;
            for (TableExecutor.SelectedColumn selectedColumn : selectedColumns) {
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
        tableExecutor.close();
    }

    /**
     * Download by primary keys
     */
    public void download(Set<List<Object>> pks) {
        try (CSVPrinter csvPrinter = outputFile.createCsvPrinter()) {
            tableExecutor.execute(pks, resultSet -> consumeResultSet(csvPrinter, resultSet));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Download the whole table
     */
    public void download() {
        try (CSVPrinter csvPrinter = outputFile.createCsvPrinter()) {
            tableExecutor.execute(resultSet -> consumeResultSet(csvPrinter, resultSet));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void consumeResultSet(CSVPrinter csvPrinter, ResultSet resultSet) {
        if (isExistingPk(resultSet)) return;

        try {
            for (TableExecutor.SelectedColumn selectedColumn : selectedColumns) {
                if (selectedColumn != null) {
                    Integer integer = selectedColumn.columnType().toSqlType();
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
                } else {
                    csvPrinter.print(null);
                }
            }
            csvPrinter.println();
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if the PK in the ResultSet has already been downloaded
     */
    private boolean isExistingPk(ResultSet resultSet) {
        if (tablePrimaryKeys == null) return false;
        return !tablePrimaryKeys.addPrimaryKey(resultSet);
    }

    private void downloadClob(ResultSet resultSet, CSVPrinter csvPrinter, String columnName, int jdbcPos) throws SQLException, IOException {
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

    private void downloadBlob(ResultSet resultSet, CSVPrinter csvPrinter, String columnName, int jdbcPos) throws SQLException, IOException {
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

    private void downloadBinary(ResultSet resultSet, CSVPrinter csvPrinter, String columnName, int jdbcPos) throws SQLException, IOException {
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

    private void downloadString(ResultSet resultSet, CSVPrinter csvPrinter, String columnName, int jdbcPos) throws SQLException, IOException {
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

    private void downloadTooLarge(CSVPrinter csvPrinter, String columnName, long length) throws IOException {
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
        private boolean byPrimaryKey;

        public Builder setByPrimaryKey() {
            return setByPrimaryKey(true);
        }

        public TableDownloader build() {
            if (database == null) throw new RuntimeException("database not set");
            if (datasetsDirectory == null) throw new RuntimeException("datasetsDirectory not set");
            if (dataset == null) throw new RuntimeException("dataset not set");
            if (tableName == null) throw new RuntimeException("tableName not set");

            return createTableDownloader(database, datasetsDirectory, dataset, tableName, byPrimaryKey);
        }
    }
}
