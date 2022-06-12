package org.dandoy.dbpop;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This strategy drops and re-creates the indexes and constraints.
 * TODO: the indexes and constraints are not perfectly re-created, they are missing cascade deletes, filters, ...
 */
public class SqlServerDisablePreparationStrategy extends DatabasePreparationStrategy {
    private final SqlServerDatabase database;
    private final Set<ForeignKey> foreignKeys;

    private SqlServerDisablePreparationStrategy(SqlServerDatabase database, Set<ForeignKey> foreignKeys) {
        this.database = database;
        this.foreignKeys = foreignKeys;
    }

    public static SqlServerDisablePreparationStrategy createPreparationStrategy(SqlServerDatabase sqlServerDatabase, Map<TableName, Table> tablesByName, Set<Table> loadedTables) {
        Set<ForeignKey> foreignKeys = getAffectedForeignKeys(tablesByName, loadedTables);
        foreignKeys.forEach(sqlServerDatabase::disableForeignKey);
        loadedTables.forEach(sqlServerDatabase::deleteTable);

        return new SqlServerDisablePreparationStrategy(sqlServerDatabase, foreignKeys);
    }

    @Override
    public void close() {
        foreignKeys.forEach(database::enableForeignKey);
    }

    private static Set<ForeignKey> getAffectedForeignKeys(Map<TableName, Table> tablesByName, Set<Table> tables) {
        return tables.stream()
                .map(Table::getTableName)
                .map(tablesByName::get)
                .filter(Objects::nonNull)
                .flatMap(table -> table.getForeignKeys().stream())
                .collect(Collectors.toSet());
    }
}
