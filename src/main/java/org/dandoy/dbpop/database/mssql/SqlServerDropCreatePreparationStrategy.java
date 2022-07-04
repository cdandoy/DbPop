package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.*;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This strategy drops and re-creates the indexes and constraints.
 * TODO: the indexes and constraints are not perfectly re-created, they are missing cascade deletes, filters, ...
 */
class SqlServerDropCreatePreparationStrategy extends DatabasePreparationStrategy<SqlServerDatabase> {
    private final Set<ForeignKey> affectedForeignKeys;
    private final Set<Index> affectedIndexes;

    private SqlServerDropCreatePreparationStrategy(SqlServerDatabase database, Map<TableName, Table> tablesByName) {
        super(database, tablesByName);
        this.affectedForeignKeys = tables.stream()
                .flatMap(table -> table.getForeignKeys().stream())
                .collect(Collectors.toSet());
        this.affectedIndexes = tables.stream()
                .map(Table::getIndexes)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    static SqlServerDropCreatePreparationStrategy createPreparationStrategy(SqlServerDatabase database, Map<TableName, Table> tablesByName) {
        return new SqlServerDropCreatePreparationStrategy(database, tablesByName);
    }

    @Override
    public void beforeInserts() {
        affectedForeignKeys.forEach(database::dropForeignKey);
        affectedIndexes.forEach(database::dropIndex);
        tables.forEach(database::truncateTable);
    }

    @Override
    public void afterInserts() {
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
