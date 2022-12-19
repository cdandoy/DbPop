package org.dandoy.dbpop.download2;

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

import static org.dandoy.dbpop.download2.TableExecutor.SelectedColumn;
import static org.dandoy.dbpop.download2.TableExecutor.createTableExecutor;

@Slf4j
public class TableDownloader {
    private static final int MAX_LENGTH = 1024 * 32;
    private final TableName tableName;
    private final OutputFile outputFile;
    private final TablePrimaryKeys tablePrimaryKeys;
    private final TableExecutor tableExecutor;
    private final List<SelectedColumn> selectedColumns;

    public TableDownloader(TableName tableName, OutputFile outputFile, TablePrimaryKeys tablePrimaryKeys, TableExecutor tableExecutor, List<SelectedColumn> selectedColumns) {
        this.tableName = tableName;
        this.outputFile = outputFile;
        this.tablePrimaryKeys = tablePrimaryKeys;
        this.tableExecutor = tableExecutor;
        this.selectedColumns = selectedColumns;
    }

    public static TableDownloader createTableDownloader(Database database, File datasetsDirectory, String dataset, TableName tableName) {
        OutputFile outputFile = OutputFile.createOutputFile(datasetsDirectory, dataset, tableName);
        Table table = database.getTable(tableName);
        TableExecutor tableExecutor = createTableExecutor(database, table);
        List<SelectedColumn> selectedColumns = tableExecutor.getSelectedColumns();
        TablePrimaryKeys tablePrimaryKeys = TablePrimaryKeys.createTablePrimaryKeys(datasetsDirectory, dataset, table, selectedColumns);

        if (outputFile.isNewFile()) {
            outputFile.setColumns(selectedColumns);
        } else {
            selectedColumns = filterSelectedColumns(selectedColumns, outputFile.getHeaders());
        }

        return new TableDownloader(tableName, outputFile, tablePrimaryKeys, tableExecutor, selectedColumns);
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

    public void download(Set<List<Object>> pks) {
        try (CSVPrinter csvPrinter = outputFile.createCsvPrinter()) {
            tableExecutor.execute(pks, resultSet -> {
                if (tablePrimaryKeys != null) {
                    if (!tablePrimaryKeys.addPrimaryKey(resultSet)) {
                        return;
                    }
                }
                for (SelectedColumn selectedColumn : selectedColumns) {
                    try {
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
                    } catch (SQLException | IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
}
