package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.upload.Dataset;

import java.util.*;

/**
 * This strategy disabled the foreign keys
 */
class SqlServerDisablePreparationStrategy extends DatabasePreparationStrategy {
    private final SqlServerDatabase database;
    private final Collection<Table> tablesToDelete;
    private final Set<ForeignKey> foreignKeys;

    public SqlServerDisablePreparationStrategy(SqlServerDatabase database, Collection<Table> tablesToDelete, Set<ForeignKey> foreignKeys) {
        this.database = database;
        this.tablesToDelete = tablesToDelete;
        this.foreignKeys = foreignKeys;
    }

    public static SqlServerDisablePreparationStrategy createPreparationStrategy(SqlServerDatabase sqlServerDatabase, Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, List<String> datasets) {
        Set<TableName> tableNamesToDelete = getTableNamesToDelete(datasetsByName, tablesByName, datasets);

        Set<ForeignKey> foreignKeys = getForeignKeysToSuppress(tablesByName, tableNamesToDelete);

        Collection<Table> tablesToDelete = toTables(tablesByName, tableNamesToDelete);

        return new SqlServerDisablePreparationStrategy(sqlServerDatabase, tablesToDelete, foreignKeys);
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
