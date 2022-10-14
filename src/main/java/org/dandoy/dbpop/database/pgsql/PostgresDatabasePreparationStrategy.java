package org.dandoy.dbpop.database.pgsql;

import org.dandoy.dbpop.database.DatabasePreparationStrategy;
import org.dandoy.dbpop.database.ForeignKey;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.upload.Dataset;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PostgresDatabasePreparationStrategy extends DatabasePreparationStrategy {

    private final PostgresDatabase database;
    private final Collection<Table> tablesToDelete;
    private final Set<ForeignKey> foreignKeys;

    public PostgresDatabasePreparationStrategy(PostgresDatabase database, Collection<Table> tablesToDelete, Set<ForeignKey> foreignKeys) {
        this.database = database;
        this.tablesToDelete = tablesToDelete;
        this.foreignKeys = foreignKeys;
    }

    public static PostgresDatabasePreparationStrategy createPreparationStrategy(PostgresDatabase database, Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, List<String> datasets) {
        Set<TableName> tableNamesToDelete = getTableNamesToDelete(datasetsByName, tablesByName, datasets);

        // We must disable every foreignKey except those that point to the static dataset
        Set<ForeignKey> foreignKeys = getForeignKeysToSuppress(tablesByName, tableNamesToDelete);

        Collection<Table> tablesToDelete = toTables(tablesByName, tableNamesToDelete);

        return new PostgresDatabasePreparationStrategy(database, tablesToDelete, foreignKeys);
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