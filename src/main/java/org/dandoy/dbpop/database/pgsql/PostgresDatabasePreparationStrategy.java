package org.dandoy.dbpop.database.pgsql;

import org.dandoy.dbpop.database.DatabasePreparationStrategy;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;

import java.util.Map;

public class PostgresDatabasePreparationStrategy extends DatabasePreparationStrategy<PostgresDatabase> {
    public PostgresDatabasePreparationStrategy(PostgresDatabase database, Map<TableName, Table> tablesByName) {
        super(database, tablesByName);
    }

    @Override
    public void beforeInserts() {
        foreignKeys.forEach(database::dropForeignKey);
        super.beforeInserts();
    }

    @Override
    public void afterInserts() {
        foreignKeys.forEach(database::createForeignKey);
        super.afterInserts();
    }
}