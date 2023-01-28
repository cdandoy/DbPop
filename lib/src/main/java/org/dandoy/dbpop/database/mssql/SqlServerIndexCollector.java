package org.dandoy.dbpop.database.mssql;

import lombok.Getter;
import org.dandoy.dbpop.database.utils.IndexCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Getter
public class SqlServerIndexCollector extends IndexCollector {
    private final Consumer<SqlServerIndexCollector> consumer;
    private String typeDesc;
    private List<SqlServerIndex.SqlServerIndexColumn> sqlServerIndexColumns;

    public SqlServerIndexCollector(Consumer<SqlServerIndexCollector> consumer) {
        super(null);
        this.consumer = consumer;
    }

    public void push(String schema, String table, String index, boolean unique, boolean primaryKey,
                     String typeDesc, String column, boolean included) {
        super.push(schema, table, index, unique, primaryKey, column);
        this.typeDesc = typeDesc;
        sqlServerIndexColumns.add(new SqlServerIndex.SqlServerIndexColumn(column, included));
    }

    @Override
    protected void flush() {
        if (sqlServerIndexColumns != null) {
            consumer.accept(this);
        }
        sqlServerIndexColumns = new ArrayList<>();
        super.columns = new ArrayList<>();
    }
}
