package org.dandoy.dbpop.database.utils;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Getter
public class IndexCollector implements AutoCloseable {
    private final Consumer<IndexCollector> indexConsumer;
    private String schema;
    private String table;
    private String name;
    private boolean unique;
    private boolean primaryKey;
    protected List<String> columns;

    public IndexCollector(Consumer<IndexCollector> indexConsumer) {
        this.indexConsumer = indexConsumer;
    }

    public void push(String schema, String table, String index, boolean unique, boolean primaryKey, String column) {
        if (!(index.equals(name) && table.equals(this.table) && schema.equals(this.schema))) {
            flush();
            this.schema = schema;
            this.table = table;
            this.name = index;
            this.unique = unique;
            this.primaryKey = primaryKey;
        }
        columns.add(column);
    }

    @Override
    public void close() {
        flush();
    }

    protected void flush() {
        if (columns != null) {
            indexConsumer.accept(this);
        }
        columns = new ArrayList<>();
    }
}
