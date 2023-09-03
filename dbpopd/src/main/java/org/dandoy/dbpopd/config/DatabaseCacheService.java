package org.dandoy.dbpopd.config;

import io.micronaut.context.annotation.Property;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.DatabaseCache;
import org.dandoy.dbpop.database.DefaultDatabase;
import org.dandoy.dbpop.database.VirtualFkCache;

import java.io.File;

import static org.dandoy.dbpopd.utils.IOUtils.toCanonical;

@Singleton
@Slf4j
public class DatabaseCacheService {
    private final ApplicationEventPublisher<DatabaseCacheChangedEvent> databaseCacheChangedPublisher;
    private final ConnectionBuilder[] connectionBuilders = new ConnectionBuilder[2];
    private final DatabaseCache[] databaseCaches = new DatabaseCache[2];
    private final VirtualFkCache virtualFkCache;

    public DatabaseCacheService(
            @SuppressWarnings("MnInjectionPoints") @Property(name = "dbpopd.configuration.path") String configurationPath,
            ApplicationEventPublisher<DatabaseCacheChangedEvent> databaseCacheChangedPublisher
    ) {
        this.databaseCacheChangedPublisher = databaseCacheChangedPublisher;
        File configurationDir = toCanonical(new File(configurationPath));
        File vfkFile = new File(configurationDir, "vfk.json");
        virtualFkCache = VirtualFkCache.createVirtualFkCache(vfkFile);
    }

    @EventListener
    void receiveConnectionBuilderChangedEvent(ConnectionBuilderChangedEvent event) {
        ConnectionBuilder connectionBuilder = event.connectionBuilder();
        setConnectionBuilder(event.type(), connectionBuilder);
    }

    private void setDatabaseCache(ConnectionType type, DatabaseCache databaseCache) {
        DatabaseCache oldDatabaseCache = databaseCaches[type.ordinal()];
        if (oldDatabaseCache != null) {
            try {
                oldDatabaseCache.close();
            } catch (Exception e) {
                log.error("Failed to close the database cache " + type);
            }
        }
        if (oldDatabaseCache == null && databaseCache == null) return;
        databaseCaches[type.ordinal()] = databaseCache;
        databaseCacheChangedPublisher.publishEvent(
                new DatabaseCacheChangedEvent(
                        type,
                        databaseCache
                )
        );
    }

    private void setConnectionBuilder(ConnectionType type, ConnectionBuilder connectionBuilder) {
        ConnectionBuilder oldConnectionBuilder = connectionBuilders[type.ordinal()];
        if (connectionBuilder != null || oldConnectionBuilder != null) {
            connectionBuilders[type.ordinal()] = connectionBuilder;

            DatabaseCache databaseCache;
            if (connectionBuilder == null) {
                databaseCache = null;
            } else {
                databaseCache = new DatabaseCache(
                        DefaultDatabase.createDatabase(connectionBuilder),
                        virtualFkCache
                );
            }
            setDatabaseCache(type, databaseCache);
        }
    }

    public DatabaseCache getSourceDatabaseCache() {
        return databaseCaches[ConnectionType.SOURCE.ordinal()];
    }

    public DatabaseCache getSourceDatabaseCacheOrThrow() {
        DatabaseCache ret = getSourceDatabaseCache();
        if (ret == null) throw new HttpStatusException(HttpStatus.BAD_REQUEST, "Source database is not connected");
        return ret;
    }

    public DatabaseCache getTargetDatabaseCache() {
        return databaseCaches[ConnectionType.TARGET.ordinal()];
    }

    public DatabaseCache getTargetDatabaseCacheOrThrow() {
        DatabaseCache ret = getTargetDatabaseCache();
        if (ret == null) throw new HttpStatusException(HttpStatus.BAD_REQUEST, "Target database is not connected");
        return ret;
    }

    public void clearTargetDatabaseCache() {
        ConnectionBuilder connectionBuilder = connectionBuilders[ConnectionType.TARGET.ordinal()];
        setConnectionBuilder(ConnectionType.TARGET, connectionBuilder);
    }
}
