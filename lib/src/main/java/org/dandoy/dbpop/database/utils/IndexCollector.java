package org.dandoy.dbpop.database.utils;

import java.util.ArrayList;
import java.util.List;

public class IndexCollector implements AutoCloseable {
    private final IndexConsumer indexConsumer;
    private String lastSchema;
    private String lastTable;
    private String lastIndex;
    private boolean lastUnique;
    private boolean lastPrimaryKey;
    private List<String> columns;

    public IndexCollector(IndexConsumer indexConsumer) {
        this.indexConsumer = indexConsumer;
    }

    public void push(String schema, String table, String index, boolean unique, boolean primaryKey, String column) {
        if (!(index.equals(lastIndex) && table.equals(lastTable) && schema.equals(lastSchema))) {
            flush();
            lastSchema = schema;
            lastTable = table;
            lastIndex = index;
            lastUnique = unique;
            lastPrimaryKey = primaryKey;
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
            indexConsumer.consume(lastSchema, lastTable, lastIndex, lastUnique, lastPrimaryKey, columns);
        }
    }

    public interface IndexConsumer {
        void consume(String schema, String table, String index, boolean unique, boolean primaryKey, List<String> columns);
    }
}
