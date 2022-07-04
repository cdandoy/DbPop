package org.dandoy.dbpop.database.utils;

import org.dandoy.dbpop.database.Column;

import java.util.List;

public interface TableConsumer {
    void consume(String schema, String table, List<Column> columns);
}
