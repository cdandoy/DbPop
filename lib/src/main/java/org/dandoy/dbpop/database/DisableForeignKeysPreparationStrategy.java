package org.dandoy.dbpop.database;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.utils.ElapsedStopWatch;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * This strategy disabled the foreign keys.
 */
@Slf4j
public class DisableForeignKeysPreparationStrategy extends DatabasePreparationStrategy {
    private final Database database;
    private final Collection<TableName> tablesToDelete;
    private final Set<ForeignKey> foreignKeys;

    public DisableForeignKeysPreparationStrategy(Database database, List<TableName> tableNames) {
        this.database = database;
        this.tablesToDelete = tableNames;
        this.foreignKeys = getForeignKeysToSuppress(database, tableNames);
    }

    @Override
    public void beforeInserts() {
        ElapsedStopWatch stopWatch = new ElapsedStopWatch();
        foreignKeys.forEach(database::disableForeignKey);
        log.debug("Disabled {} foreign keys in {}", foreignKeys.size(), stopWatch);
        tablesToDelete.forEach(database::deleteTable);
        log.debug("Deleted {} tables in {}", tablesToDelete.size(), stopWatch);
    }

    @Override
    public void afterInserts() {
        ElapsedStopWatch stopWatch = new ElapsedStopWatch();
        foreignKeys.forEach(database::enableForeignKey);
        log.debug("Enabled {} foreign keys in {}", foreignKeys.size(), stopWatch);
    }
}
