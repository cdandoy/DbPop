package org.dandoy.dbpop.database.mssql;

import lombok.Getter;
import org.dandoy.dbpop.database.utils.IndexCollector;

import java.util.function.Consumer;

@Getter
public class SqlServerIndexCollector extends IndexCollector {
    private final Consumer<SqlServerIndexCollector> consumer;
    private String typeDesc;

    public SqlServerIndexCollector(Consumer<SqlServerIndexCollector> consumer) {
        super(null);
        this.consumer = consumer;
    }

    @Override
    protected void flush() {
        consumer.accept(this);
    }

    public void push(String schema, String table, String index, boolean unique, boolean primaryKey, String typeDesc, String column) {
        super.push(schema, table, index, unique, primaryKey, column);
        this.typeDesc = typeDesc;
    }
}
