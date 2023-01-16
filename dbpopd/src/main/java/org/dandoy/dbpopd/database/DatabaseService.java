package org.dandoy.dbpopd.database;

import io.micronaut.context.annotation.Context;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.Getter;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpopd.ConfigurationService;

import java.util.*;

@SuppressWarnings("DuplicatedCode")
@Singleton
@Context
public class DatabaseService {
    private final ConfigurationService configurationService;
    private final Object sourceTablesLock = new Object();
    private final Object targetTablesLock = new Object();
    private final Object sourceRowCountLock = new Object();
    private final Object targetRowCountLock = new Object();

    private Map<TableName, Table> sourceTables = new HashMap<>();
    private Map<TableName, Table> targetTables = new HashMap<>();
    @Getter
    private Map<TableName, RowCount> sourceRowCounts = new HashMap<>();
    @Getter
    private Map<TableName, RowCount> targetRowCounts = new HashMap<>();

    public DatabaseService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @PostConstruct
    void postConstruct() {
        new Thread(this::load).start();
    }

    private void load() {
        loadSourceTables();
        loadTargetTables();
        loadSourceTableCount();
        loadTargetTableCount();
    }

    private void loadSourceTables() {
        if (configurationService.hasSourceConnection()) {
            synchronized (sourceTablesLock) {
                try (Database database = configurationService.createSourceDatabase()) {
                    Map<TableName, Table> tables = new HashMap<>();
                    database.getTables().forEach(table -> tables.put(table.tableName(), table));
                    sourceTables = tables;
                }
            }
        }
    }

    private void loadTargetTables() {
        if (configurationService.hasTargetConnection()) {
            synchronized (targetTablesLock) {
                try (Database database = configurationService.createTargetDatabase()) {
                    Map<TableName, Table> tables = new HashMap<>();
                    database.getTables().forEach(table -> tables.put(table.tableName(), table));
                    targetTables = tables;
                }
            }
        }
    }

    private void loadSourceTableCount() {
        if (configurationService.hasSourceConnection()) {
            synchronized (sourceRowCountLock) {
                try (Database database = configurationService.createSourceDatabase()) {
                    Map<TableName, RowCount> rowCounts = new HashMap<>();
                    for (Map.Entry<TableName, Table> entry : sourceTables.entrySet()) {
                        TableName tableName = entry.getKey();
                        RowCount rowCount = database.getRowCount(tableName);
                        rowCounts.put(tableName, rowCount);
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
                    for (Map.Entry<TableName, Table> entry : targetTables.entrySet()) {
                        TableName tableName = entry.getKey();
                        RowCount rowCount = database.getRowCount(tableName);
                        rowCounts.put(tableName, rowCount);
                    }
                    targetRowCounts = rowCounts;
                }
            }
        }
    }

    public Set<TableName> getTargetTableNames() {
        synchronized (targetTablesLock) {
            return targetTables.keySet();
        }
    }

    public Collection<Table> getSourceTables() {
        synchronized (sourceTablesLock) {
            return sourceTables.values();
        }
    }

    public Table getSourceTable(TableName tableName) {
        synchronized (sourceTablesLock) {
            return sourceTables.get(tableName);
        }
    }

    public RowCount getSourceRowCount(TableName tableName) {
        return sourceRowCounts.get(tableName);
    }

    public RowCount getTargetRowCount(TableName tableName) {
        return targetRowCounts.get(tableName);
    }

    public List<ForeignKey> getRelatedSourceForeignKeys(TableName pkTableName) {
        synchronized (sourceTablesLock) {
            return sourceTables.values().stream()
                    .flatMap(it -> it.foreignKeys().stream())
                    .filter(it -> it.getPkTableName().equals(pkTableName))
                    .toList();
        }
    }
}
