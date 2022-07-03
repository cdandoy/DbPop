package org.dandoy.dbpop.database;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This strategy drops and re-creates the indexes and constraints.
 * TODO: the indexes and constraints are not perfectly re-created, they are missing cascade deletes, filters, ...
 */
class SqlServerDropCreatePreparationStrategy extends DatabasePreparationStrategy {
    private final Database database;
    private final Set<ForeignKey> affectedForeignKeys;
    private final Set<Index> affectedIndexes;

    private SqlServerDropCreatePreparationStrategy(Database database, Set<ForeignKey> affectedForeignKeys, Set<Index> affectedIndexes) {
        this.database = database;
        this.affectedForeignKeys = affectedForeignKeys;
        this.affectedIndexes = affectedIndexes;
    }

    static SqlServerDropCreatePreparationStrategy createPreparationStrategy(Database database, Map<TableName, Table> tablesByName) {
        Collection<Table> tables = tablesByName.values();
        Set<ForeignKey> foreignKeys = tables.stream()
                .flatMap(table -> table.getForeignKeys().stream())
                .collect(Collectors.toSet());

        Set<Index> indexes = tables.stream()
                .map(Table::getIndexes)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());


        foreignKeys.forEach(database::dropForeignKey);
        indexes.forEach(database::dropIndex);
        tables.forEach(database::truncateTable);

        return new SqlServerDropCreatePreparationStrategy(database, foreignKeys, indexes);
    }

    @Override
    public void close() {
        createIndexes(affectedIndexes);
        createForeignKeys(affectedForeignKeys);
    }

    private void createIndexes(Set<Index> indexes) {
        indexes.forEach(database::createIndex);
    }

    private void createForeignKeys(Set<ForeignKey> foreignKeys) {
        foreignKeys.forEach(database::createForeignKey);
    }
}
