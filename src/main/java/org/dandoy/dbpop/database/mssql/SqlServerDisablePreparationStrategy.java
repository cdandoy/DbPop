package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.DatabasePreparationStrategy;
import org.dandoy.dbpop.database.ForeignKey;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This strategy disabled the foreign keys
 */
class SqlServerDisablePreparationStrategy extends DatabasePreparationStrategy<SqlServerDatabase> {
    private final Set<ForeignKey> foreignKeys;

    private SqlServerDisablePreparationStrategy(SqlServerDatabase database, Map<TableName, Table> tablesByName) {
        super(database, tablesByName);
        this.foreignKeys = tables.stream()
                .flatMap(table -> table.getForeignKeys().stream())
                .collect(Collectors.toSet());
    }

    static SqlServerDisablePreparationStrategy createPreparationStrategy(SqlServerDatabase sqlServerDatabase, Map<TableName, Table> tablesByName) {
        return new SqlServerDisablePreparationStrategy(sqlServerDatabase, tablesByName);
    }

    @Override
    public void beforeInserts() {
        foreignKeys.forEach(database::disableForeignKey);
        super.beforeInserts();
    }

    @Override
    public void afterInserts() {
        super.afterInserts();
        foreignKeys.forEach(database::enableForeignKey);
    }
}
