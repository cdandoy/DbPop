package org.dandoy.dbpopd.database;

import io.micronaut.context.annotation.Context;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import lombok.Getter;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpopd.ConfigurationService;

import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("DuplicatedCode")
@Singleton
@Context
public class DatabaseService {
    private final ConfigurationService configurationService;
    private final DatabaseCache sourceDatabase;
    private final DatabaseCache targetDatabase;
    private final Object sourceRowCountLock = new Object();
    private final Object targetRowCountLock = new Object();

    @Getter
    private Map<TableName, RowCount> sourceRowCounts = new HashMap<>();
    @Getter
    private Map<TableName, RowCount> targetRowCounts = new HashMap<>();

    public DatabaseService(ConfigurationService configurationService) {
        this.configurationService = configurationService;

        if (configurationService.hasSourceConnection()) {
            VirtualFkCache virtualFkCache = configurationService.getVirtualFkCache();
            ConnectionBuilder sourceConnectionBuilder = configurationService.getSourceConnectionBuilder();
            DefaultDatabase defaultDatabase = Database.createDefaultDatabase(sourceConnectionBuilder);
            sourceDatabase = new DatabaseCache(defaultDatabase, virtualFkCache);
        } else {
            sourceDatabase = null;
        }

        if (configurationService.hasTargetConnection()) {
            ConnectionBuilder targetConnectionBuilder = configurationService.getTargetConnectionBuilder();
            DefaultDatabase defaultDatabase = Database.createDefaultDatabase(targetConnectionBuilder);
            targetDatabase = new DatabaseCache(defaultDatabase, VirtualFkCache.createVirtualFkCache());
        } else {
            targetDatabase = null;
        }
    }

    @PostConstruct
    void postConstruct() {
        new Thread(this::load).start();
    }

    @PreDestroy
    void preDestroy() {
        if (sourceDatabase != null) sourceDatabase.close();
        if (targetDatabase != null) targetDatabase.close();
    }

    private void load() {
        loadSourceTableCount();
        loadTargetTableCount();
    }

    private void loadSourceTableCount() {
        if (configurationService.hasSourceConnection()) {
            synchronized (sourceRowCountLock) {
                try (Database database = configurationService.createSourceDatabase()) {
                    Map<TableName, RowCount> rowCounts = new HashMap<>();
                    for (String catalog : sourceDatabase.getCatalogs()) {
                        if ("tempdb".equals(catalog)) continue;
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

    private void loadTargetTableCount() {
        if (configurationService.hasTargetConnection()) {
            synchronized (targetRowCountLock) {
                try (Database database = configurationService.createTargetDatabase()) {
                    Map<TableName, RowCount> rowCounts = new HashMap<>();
                    for (Table table : database.getTables()) {
                        TableName tableName = table.getTableName();
                        RowCount rowCount = database.getRowCount(tableName);
                        rowCounts.put(tableName, rowCount);
                    }
                    targetRowCounts = rowCounts;
                }
            }
        }
    }

    public Set<TableName> getTargetTableNames() {
        return targetDatabase.getTables().stream()
                .map(Table::getTableName)
                .collect(Collectors.toSet());
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

    public RowCount getTargetRowCount(TableName tableName) {
        return targetRowCounts.get(tableName);
    }

    public List<ForeignKey> getRelatedSourceForeignKeys(TableName pkTableName) {
        return sourceDatabase.getRelatedForeignKeys(pkTableName);
    }
}
