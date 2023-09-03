package org.dandoy.dbpopd.database;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.ConnectionBuilderChangedEvent;
import org.dandoy.dbpopd.config.ConnectionType;

import java.util.Collection;
import java.util.List;

@Singleton
@Slf4j
public class DatabaseService {
    private final ConfigurationService configurationService;
    @Nullable
    private DatabaseCache sourceDatabase;

    public DatabaseService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @PreDestroy
    void preDestroy() {
        if (sourceDatabase != null) sourceDatabase.close();
    }

    @EventListener
    void receiveConnectionBuilderChangedEvent(ConnectionBuilderChangedEvent event) {
        if (event.type() == ConnectionType.SOURCE) {
            ConnectionBuilder sourceConnectionBuilder = event.connectionBuilder();
            if (sourceConnectionBuilder != null) {
                VirtualFkCache virtualFkCache = configurationService.getVirtualFkCache();
                log.info("Checking the source connection");
                DefaultDatabase defaultDatabase = Database.createDefaultDatabase(sourceConnectionBuilder);
                sourceDatabase = new DatabaseCache(defaultDatabase, virtualFkCache);
            } else {
                sourceDatabase = null;
            }
        }
    }

    public Collection<Table> getSourceTables() {
        if (sourceDatabase == null) throw new HttpStatusException(HttpStatus.BAD_REQUEST, "Source database not available");
        return sourceDatabase.getTables();
    }

    public Table getSourceTable(TableName tableName) {
        if (sourceDatabase == null) throw new HttpStatusException(HttpStatus.BAD_REQUEST, "Source database not available");
        return sourceDatabase.getTable(tableName);
    }

    @Deprecated
    public RowCount getSourceRowCount(TableName tableName) {
        return new RowCount(99, false);
    }

    public List<ForeignKey> getRelatedSourceForeignKeys(TableName pkTableName) {
        if (sourceDatabase == null) throw new HttpStatusException(HttpStatus.BAD_REQUEST, "Source database not available");
        return sourceDatabase.getRelatedForeignKeys(pkTableName);
    }
}
