package org.dandoy.dbpop.database;

import java.util.Set;

public interface DatabasePreparationFactory {
    DatabasePreparationStrategy createDatabasePreparationStrategy(Database database, Set<TableName> tableNames);
}
