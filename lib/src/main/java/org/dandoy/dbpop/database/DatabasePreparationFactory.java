package org.dandoy.dbpop.database;

import java.util.List;

public interface DatabasePreparationFactory {
    DatabasePreparationStrategy createDatabasePreparationStrategy(Database database, List<TableName> tablesToDelete);
}
