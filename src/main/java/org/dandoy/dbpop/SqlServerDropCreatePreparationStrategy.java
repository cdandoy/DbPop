package org.dandoy.dbpop;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This strategy drops and re-creates the indexes and constraints.
 * TODO: the indexes and constraints are not perfectly re-created, they are missing cascade deletes, filters, ...
 */
public class SqlServerDropCreatePreparationStrategy extends DatabasePreparationStrategy {
    private final Database database;
    private final Set<ForeignKey> affectedForeignKeys;
    private final Set<Index> affectedIndexes;

    private SqlServerDropCreatePreparationStrategy(Database database, Set<ForeignKey> affectedForeignKeys, Set<Index> affectedIndexes) {
        this.database = database;
        this.affectedForeignKeys = affectedForeignKeys;
        this.affectedIndexes = affectedIndexes;
    }

    public static SqlServerDropCreatePreparationStrategy createPreparationStrategy(Database database, Map<TableName, Table> tablesByName, Set<Table> loadedTables) {
        Set<ForeignKey> affectedForeignKeys = getAffectedForeignKeys(tablesByName, loadedTables);
        Set<Index> affectedIndexes = getAffectedIndexes(loadedTables);

        SqlServerDropCreatePreparationStrategy strategy = new SqlServerDropCreatePreparationStrategy(database, affectedForeignKeys, affectedIndexes);

        strategy.dropForeignKeys(affectedForeignKeys);
        strategy.dropIndexes(affectedIndexes);
        strategy.truncateTables(loadedTables);

        return strategy;
    }

    @Override
    public void close() {
        createIndexes(affectedIndexes);
        createForeignKeys(affectedForeignKeys);
    }

    private static Set<ForeignKey> getAffectedForeignKeys(Map<TableName, Table> tablesByName, Set<Table> tables) {
        return tables.stream()
                .map(Table::getTableName)
                .map(tablesByName::get)
                .filter(Objects::nonNull)
                .flatMap(table -> table.getForeignKeys().stream())
                .collect(Collectors.toSet());
    }

    private static Set<Index> getAffectedIndexes(Set<Table> tables) {
        return tables.stream()
                .map(Table::getIndexes)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private void dropForeignKeys(Set<ForeignKey> foreignKeys) {
        foreignKeys.forEach(database::dropForeignKey);
    }

    private void dropIndexes(Set<Index> indexes) {
        indexes.forEach(database::dropIndex);
    }

    private void createIndexes(Set<Index> indexes) {
        indexes.forEach(database::createIndex);
    }

    private void createForeignKeys(Set<ForeignKey> foreignKeys) {
        foreignKeys.forEach(database::createForeignKey);
    }

    private void truncateTables(Set<Table> tables) {
        tables.forEach(database::truncateTable);
    }
}
