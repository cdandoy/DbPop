package org.dandoy.dbpop;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This strategy drops and re-creates the indexes and constraints.
 * TODO: the indexes and constraints are not perfectly re-created, they are missing cascade deletes, filters, ...
 */
class SqlServerDisablePreparationStrategy extends DatabasePreparationStrategy {
    private final SqlServerDatabase database;
    private final Set<ForeignKey> foreignKeys;

    private SqlServerDisablePreparationStrategy(SqlServerDatabase database, Set<ForeignKey> foreignKeys) {
        this.database = database;
        this.foreignKeys = foreignKeys;
    }

    static SqlServerDisablePreparationStrategy createPreparationStrategy(SqlServerDatabase sqlServerDatabase, Map<TableName, Table> tablesByName) {
        Collection<Table> tables = tablesByName.values();
        Set<ForeignKey> foreignKeys = tables.stream()
                .flatMap(table -> table.getForeignKeys().stream())
                .collect(Collectors.toSet());

        foreignKeys.forEach(sqlServerDatabase::disableForeignKey);
        tables.forEach(sqlServerDatabase::deleteTable);

        return new SqlServerDisablePreparationStrategy(sqlServerDatabase, foreignKeys);
    }

    @Override
    public void close() {
        foreignKeys.forEach(database::enableForeignKey);
    }
}
