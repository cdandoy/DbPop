package org.dandoy.dbpop.database;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A DatabasePreparationStrategy prepares the tables for deletion and insertion.
 */
public abstract class DatabasePreparationStrategy<D extends Database> {
    protected final D database;
    protected final Collection<Table> tables;
    protected final Set<ForeignKey> foreignKeys;

    public DatabasePreparationStrategy(D database, Map<TableName, Table> tablesByName) {
        this.database = database;
        this.tables = tablesByName.values();
        this.foreignKeys = tables.stream()
                .flatMap(table -> table.getForeignKeys().stream())
                .collect(Collectors.toSet());
    }

    public void beforeInserts() {
        tables.forEach(database::deleteTable);
    }

    public void afterInserts() {
    }
}
