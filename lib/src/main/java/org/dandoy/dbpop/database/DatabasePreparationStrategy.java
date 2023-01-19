package org.dandoy.dbpop.database;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A DatabasePreparationStrategy prepares the tables for deletion and insertion.
 */
public abstract class DatabasePreparationStrategy {

    @NotNull
    protected static Set<ForeignKey> getForeignKeysToSuppress(Database database, Set<TableName> tableNames) {
        return Stream.concat(
                tableNames.stream()
                        .map(tableName -> database.getTable(tableName).foreignKeys())
                        .flatMap(Collection::stream),
                tableNames.stream()
                        .map(database::getRelatedForeignKeys)
                        .flatMap(Collection::stream)
        ).collect(Collectors.toSet());
    }

    public abstract void beforeInserts();

    public abstract void afterInserts();
}
