package org.dandoy.dbpop.database;

import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;

import java.util.*;
import java.util.stream.Collectors;

import static org.dandoy.dbpop.datasets.Datasets.STATIC;

/**
 * A DatabasePreparationStrategy prepares the tables for deletion and insertion.
 */
public abstract class DatabasePreparationStrategy {
    protected static Collection<Table> toTables(Map<TableName, Table> tablesByName, Set<TableName> tableNamesToDelete) {
        return tableNamesToDelete.stream().map(tablesByName::get).collect(Collectors.toList());
    }

    /**
     * We must disable every foreignKey pointing to a table we will delete
     */
    protected static Set<ForeignKey> getForeignKeysToSuppress(Map<TableName, Table> tablesByName, Set<TableName> tableNamesToDelete) {
        Set<ForeignKey> foreignKeys = new HashSet<>();
        for (Table table : tablesByName.values()) {
            for (ForeignKey foreignKey : table.getForeignKeys()) {
                TableName pkTableName = foreignKey.getPkTableName();
                if (tableNamesToDelete.contains(pkTableName)) {
                    foreignKeys.add(foreignKey);
                }
            }
        }
        return foreignKeys;
    }

    protected static Set<TableName> getTableNamesToDelete(Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, List<String> datasets) {
        // We must delete from every table except those in the static dataset unless it is loaded too
        Set<TableName> tableNamesToDelete = new HashSet<>(tablesByName.keySet());
        if (!datasets.contains(STATIC)) {
            Dataset staticDataset = datasetsByName.get(STATIC);
            if (staticDataset != null) {
                for (DataFile dataFile : staticDataset.getDataFiles()) {
                    TableName tableName = dataFile.getTableName();
                    tableNamesToDelete.remove(tableName);
                }
            }
        }
        return tableNamesToDelete;
    }

    public abstract void beforeInserts();

    public abstract void afterInserts();
}
