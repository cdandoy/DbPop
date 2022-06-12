package org.dandoy.dbpop;

import java.util.Map;
import java.util.Set;

public abstract class DatabasePreparationStrategy implements AutoCloseable {
    public static DatabasePreparationStrategy createDatabasePreparationStrategy(Database database, Map<TableName, Table> tablesByName, Set<Table> loadedTables) {
        if (database instanceof SqlServerDatabase) {
            SqlServerDatabase sqlServerDatabase = (SqlServerDatabase) database;
            if (Settings.DISABLE_CONTRAINTS) {
                return SqlServerDisablePreparationStrategy.createPreparationStrategy(sqlServerDatabase, tablesByName, loadedTables);
            } else {
                return SqlServerDropCreatePreparationStrategy.createPreparationStrategy(sqlServerDatabase, tablesByName, loadedTables);
            }
        } else {
            throw new RuntimeException("Not implemented");
        }
    }

    @Override
    public abstract void close();
}
