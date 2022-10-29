package org.dandoy.dbpop.database.utils;

import org.dandoy.dbpop.database.Column;

import java.util.ArrayList;
import java.util.List;

public class TableCollector implements AutoCloseable {
    final TableConsumer tableConsumer;
    private String lastSchema;
    private String lastTable;
    private List<Column> columns;

    public  TableCollector(TableConsumer tableConsumer) {
        this.tableConsumer = tableConsumer;
    }

    public  void push(String schema, String table, Column column) {
        if (!(table.equals(lastTable) && schema.equals(lastSchema))) {
            flush();
            lastSchema = schema;
            lastTable = table;
            columns = new ArrayList<>();
        }
        columns.add(column);
    }

    @Override
    public void close() {
        flush();
    }

    private void flush() {
        if (columns != null) {
            tableConsumer.consume(lastSchema, lastTable, columns);
        }
    }
}
