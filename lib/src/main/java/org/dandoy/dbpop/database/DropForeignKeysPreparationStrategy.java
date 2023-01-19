package org.dandoy.dbpop.database;

import java.util.Collection;
import java.util.Set;

public class DropForeignKeysPreparationStrategy extends DatabasePreparationStrategy {
    private final Database database;
    private final Collection<TableName> tablesToDelete;
    private final Set<ForeignKey> foreignKeys;

    public DropForeignKeysPreparationStrategy(Database database, Set<TableName> tableNames) {
        this.database = database;
        this.tablesToDelete = tableNames;
        this.foreignKeys = getForeignKeysToSuppress(database, tableNames);
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