package org.dandoy.dbpop.download;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.TableName;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

@Slf4j
public class ExecutionNode implements Consumer<ResultSet>, AutoCloseable {
    public static final int BATCH_SIZE = 1000;
    @Getter
    private final TableName tableName;
    private final TableDownloader tableDownloader;
    private final List<SelectedColumn> extractedColumns;
    private final Set<List<Object>> keys = new HashSet<>();

    public ExecutionNode(TableName tableName, TableDownloader tableDownloader, List<SelectedColumn> extractedColumns) {
        this.tableName = tableName;
        this.tableDownloader = tableDownloader;
        this.extractedColumns = extractedColumns;
    }

    @Override
    public String toString() {
        return "ExecutionNode{" +
               "tableName=" + tableName +
               ";buffered=" + keys.size() +
               ";rowCount=" + tableDownloader.getRowCount() +
               '}';
    }

    @Override
    public void close() {
        tableDownloader.close();
    }

    public int getRowCount() {
        return tableDownloader.getRowCount();
    }

    public void download(Set<List<Object>> pks) {
        tableDownloader.download(pks);
    }

    public List<SelectedColumn> getSelectedColumns() {
        return tableDownloader.getSelectedColumns();
    }

    @Override
    public void accept(ResultSet resultSet) {
        log.debug("{} receives a row", tableName);
        try {
            List<Object> values = new ArrayList<>();
            for (SelectedColumn selectedColumn : extractedColumns) {
                Object value = resultSet.getObject(selectedColumn.jdbcPos());
                if (value == null) return;
                values.add(value);
            }
            keys.add(values);
            if (keys.size() > BATCH_SIZE) {
                flush();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean flush() {
        if (keys.isEmpty()) return false;
        log.debug("{} flushes {} rows", tableName, keys.size());
        tableDownloader.download(keys);
        keys.clear();
        return true;
    }

    public void addExecutionNode(ExecutionNode childExecutionNode) {
        tableDownloader.addConsumer(childExecutionNode);
    }
}
