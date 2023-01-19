package org.dandoy.dbpop.database;

import org.dandoy.dbpop.upload.Dataset;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DropForeignKeysPreparationStrategy extends DatabasePreparationStrategy {
    private final Database database;
    private final Collection<TableName> tablesToDelete;
    private final Set<ForeignKey> foreignKeys;

    public DropForeignKeysPreparationStrategy(Database database, Collection<Table> tablesToDelete, Set<ForeignKey> foreignKeys) {
        this.database = database;
        this.tablesToDelete = tablesToDelete.stream().map(Table::tableName).toList();
        this.foreignKeys = foreignKeys;
    }

    public DropForeignKeysPreparationStrategy(Database database, List<TableName> tablesToDelete) {
        this.database = database;
        this.tablesToDelete = tablesToDelete;
        this.foreignKeys = tablesToDelete.stream()
                .map(database::getRelatedForeignKeys)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    public static DropForeignKeysPreparationStrategy createPreparationStrategy(Database database, Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, List<String> datasets) {
        Set<TableName> tableNamesToDelete = getTableNamesToDelete(datasetsByName, tablesByName, datasets);

        // We must disable every foreignKey except those that point to the static dataset
        Set<ForeignKey> foreignKeys = getForeignKeysToSuppress(tablesByName, tableNamesToDelete);

        Collection<Table> tablesToDelete = toTables(tablesByName, tableNamesToDelete);

        return new DropForeignKeysPreparationStrategy(database, tablesToDelete, foreignKeys);
    }

    @Override
    public void beforeInserts() {
        foreignKeys.forEach(database::dropForeignKey);
        tablesToDelete.forEach(database::deleteTable);
    }

    @Override
    public void afterInserts() {
        foreignKeys.forEach(database::createForeignKey);
    }
}