package org.dandoy.dbpopd.config;

import io.micronaut.context.annotation.Property;
import io.micronaut.context.event.ApplicationEventListener;
import jakarta.inject.Singleton;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.DatabaseCache;
import org.dandoy.dbpop.database.DefaultDatabase;
import org.dandoy.dbpop.database.VirtualFkCache;

import java.io.File;

import static org.dandoy.dbpopd.utils.IOUtils.toCanonical;

@Singleton
public class DatabaseCacheService implements ApplicationEventListener<ConnectionBuilderChangedEvent> {
    private ConnectionBuilder sourceConnectionBuilder;
    private ConnectionBuilder targetConnectionBuilder;
    private DatabaseCache sourceDatabaseCache;
    private DatabaseCache targetDatabaseCache;
    private final VirtualFkCache virtualFkCache;

    public DatabaseCacheService(
            @SuppressWarnings("MnInjectionPoints") @Property(name = "dbpopd.configuration.path") String configurationPath
    ) {
        File configurationDir = toCanonical(new File(configurationPath));
        File vfkFile = new File(configurationDir, "vfk.json");
        virtualFkCache = VirtualFkCache.createVirtualFkCache(vfkFile);
    }

    @Override
    public void onApplicationEvent(ConnectionBuilderChangedEvent event) {
        if (event.type() == ConnectionType.SOURCE) {
            sourceConnectionBuilder = event.connectionBuilder();
            sourceDatabaseCache = null;
        } else if (event.type() == ConnectionType.TARGET) {
            targetConnectionBuilder = event.connectionBuilder();
            sourceDatabaseCache = null;
        }
    }

    public synchronized DatabaseCache getSourceDatabaseCache() {
        if (sourceDatabaseCache == null) {
            sourceDatabaseCache = new DatabaseCache(
                    DefaultDatabase.createDatabase(sourceConnectionBuilder),
                    virtualFkCache
            );
        }
        // Make sure we have a working connection
        sourceDatabaseCache.verifyConnection();

        return sourceDatabaseCache;
    }

    public void clearSourceDatabaseCache() {
        sourceDatabaseCache = null;
    }

    public void clearTargetDatabaseCache() {
        targetDatabaseCache = null;
    }

    public synchronized DatabaseCache getTargetDatabaseCache() {
        if (targetDatabaseCache == null) {
            targetDatabaseCache = new DatabaseCache(
                    DefaultDatabase.createDatabase(targetConnectionBuilder),
                    virtualFkCache
            ) {
                @Override
                public void close() {
                    throw new RuntimeException("Do not close me!");
                }
            };
        }

        // Make sure we have a working connection
        targetDatabaseCache.verifyConnection();

        return targetDatabaseCache;
    }
}
