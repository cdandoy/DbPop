package org.dandoy.dbpop.database;

import org.dandoy.dbpop.upload.Dataset;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This strategy disabled the foreign keys
 */
public class DisableForeignKeysPreparationStrategy extends DatabasePreparationStrategy {
    private final Database database;
    private final Collection<TableName> tablesToDelete;
    private final Set<ForeignKey> foreignKeys;

    public DisableForeignKeysPreparationStrategy(Database database, Collection<Table> tablesToDelete, Set<ForeignKey> foreignKeys) {
        this.database = database;
        this.tablesToDelete = tablesToDelete.stream().map(Table::tableName).toList();
        this.foreignKeys = foreignKeys;
    }

    public DisableForeignKeysPreparationStrategy(Database database, List<TableName> tablesToDelete) {
        this.database = database;
        this.tablesToDelete = tablesToDelete;
        this.foreignKeys = tablesToDelete.stream()
                .map(database::getRelatedForeignKeys)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    public static DisableForeignKeysPreparationStrategy createPreparationStrategy(Database sqlServerDatabase, Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, List<String> datasets) {
        Set<TableName> tableNamesToDelete = getTableNamesToDelete(datasetsByName, tablesByName, datasets);

        Set<ForeignKey> foreignKeys = tableNamesToDelete.stream()
                .map(sqlServerDatabase::getRelatedForeignKeys)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        Collection<Table> tablesToDelete = toTables(tablesByName, tableNamesToDelete);

        return new DisableForeignKeysPreparationStrategy(sqlServerDatabase, tablesToDelete, foreignKeys);
    }

    @Override
    public void beforeInserts() {
        foreignKeys.forEach(database::disableForeignKey);
        tablesToDelete.forEach(database::deleteTable);
    }

    @Override
    public void afterInserts() {
        foreignKeys.forEach(database::enableForeignKey);
    }
}
