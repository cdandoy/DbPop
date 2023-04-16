package org.dandoy.dbpopd.database;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpopd.ConfigurationService;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
@Slf4j
public class DatabaseService {
    private final ConfigurationService configurationService;
    private final DatabaseCache sourceDatabase;
    private final DatabaseCache targetDatabase;
    private final Object sourceRowCountLock = new Object();

    @Getter
    private Map<TableName, RowCount> sourceRowCounts = new HashMap<>();

    public DatabaseService(ConfigurationService configurationService) {
        this.configurationService = configurationService;

        if (configurationService.hasSourceConnection()) {
            VirtualFkCache virtualFkCache = configurationService.getVirtualFkCache();
            ConnectionBuilder sourceConnectionBuilder = configurationService.getSourceConnectionBuilder();
            log.info("Checking the source connection");
            DefaultDatabase defaultDatabase = Database.createDefaultDatabase(sourceConnectionBuilder);
            sourceDatabase = new DatabaseCache(defaultDatabase, virtualFkCache);
        } else {
            sourceDatabase = null;
        }

        if (configurationService.hasTargetConnection()) {
            log.info("Checking the target connection");
            ConnectionBuilder targetConnectionBuilder = configurationService.getTargetConnectionBuilder();
            DefaultDatabase defaultDatabase = Database.createDefaultDatabase(targetConnectionBuilder);
            targetDatabase = new DatabaseCache(defaultDatabase, VirtualFkCache.createVirtualFkCache());
        } else {
            targetDatabase = null;
        }
    }

    @PostConstruct
    void postConstruct() {
        new Thread(this::loadSourceTableCount).start();
    }

    @PreDestroy
    void preDestroy() {
        if (sourceDatabase != null) sourceDatabase.close();
        if (targetDatabase != null) targetDatabase.close();
    }

    private void loadSourceTableCount() {
        if (configurationService.hasSourceConnection()) {
            synchronized (sourceRowCountLock) {
                try (Database database = configurationService.createSourceDatabase()) {
                    Map<TableName, RowCount> rowCounts = new HashMap<>();
                    for (String catalog : sourceDatabase.getCatalogs()) {
                        for (Table table : sourceDatabase.getTables(catalog)) {
                            TableName tableName = table.getTableName();
                            RowCount rowCount = database.getRowCount(tableName);
                            rowCounts.put(tableName, rowCount);
                        }
                    }
                    sourceRowCounts = rowCounts;
                }
            }
        }
    }

    public Collection<Table> getSourceTables() {
        return sourceDatabase.getTables();
    }

    public Table getSourceTable(TableName tableName) {
        return sourceDatabase.getTable(tableName);
    }

    public RowCount getSourceRowCount(TableName tableName) {
        return sourceRowCounts.get(tableName);
    }

    public List<ForeignKey> getRelatedSourceForeignKeys(TableName pkTableName) {
        return sourceDatabase.getRelatedForeignKeys(pkTableName);
    }
}
