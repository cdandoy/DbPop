package org.dandoy.dbpopd.config;

import io.micronaut.context.annotation.Property;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.VirtualFkCache;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

import static org.dandoy.dbpopd.utils.IOUtils.toCanonical;

@SuppressWarnings("unused")
@Singleton
@Slf4j
@Getter
public class ConfigurationService implements ApplicationEventListener<ConnectionBuilderChangedEvent> {
    static final String PROP_FILE_NAME = "dbpop.properties";
    private final File configurationDir;
    private ConnectionBuilder sourceConnectionBuilder;
    private ConnectionBuilder targetConnectionBuilder;
    private final File datasetsDirectory;
    private final File setupDirectory;
    private final File codeDirectory;
    private final File extensionsDirectory;
    private final VirtualFkCache virtualFkCache;
    @Setter
    private boolean codeAutoSave;

    @SuppressWarnings("MnInjectionPoints")
    public ConfigurationService(
            @Property(name = "dbpopd.configuration.path") String configurationPath,
            @Property(name = "dbpopd.configuration.datasets") @Nullable String datasetsDirectory,
            @Property(name = "dbpopd.configuration.setup") @Nullable String setupDirectory,
            @Property(name = "dbpopd.configuration.code") @Nullable String codeDirectory,
            @Property(name = "dbpopd.configuration.extensions") @Nullable String extensionsDirectory,
            @Property(name = "dbpopd.configuration.codeAutoSave", defaultValue = "false") boolean codeAutoSave
    ) {
        configurationDir = toCanonical(new File(configurationPath));
        File configurationFile = new File(configurationDir, PROP_FILE_NAME);

        File vfkFile = new File(configurationDir, "vfk.json");
        virtualFkCache = VirtualFkCache.createVirtualFkCache(vfkFile);
        this.datasetsDirectory = datasetsDirectory != null ? toCanonical(new File(datasetsDirectory)) : new File(configurationDir, "datasets");
        this.setupDirectory = setupDirectory != null ? toCanonical(new File(setupDirectory)) : new File(configurationDir, "setup");
        this.codeDirectory = codeDirectory != null ? toCanonical(new File(codeDirectory)) : new File(configurationDir, "code");
        this.extensionsDirectory = extensionsDirectory != null ? toCanonical(new File(extensionsDirectory)) : new File(configurationDir, "extensions");
        this.codeAutoSave = codeAutoSave;

        log.info("Configuration directory: {}", toCanonical(this.configurationDir));
        log.debug("Datasets Directory: {}", this.datasetsDirectory);
        log.debug("Setup Directory: {}", this.setupDirectory);
        log.debug("Code Directory: {}", this.codeDirectory);
        log.debug("Extensions Directory: {}", this.extensionsDirectory);
        log.debug("Code Auto-save: {}", this.codeAutoSave);

        if (datasetsDirectory != null) log.info("Datasets directory: {}", toCanonical(this.datasetsDirectory));
        if (setupDirectory != null) log.info("Setup directory: {}", toCanonical(this.setupDirectory));
        if (codeDirectory != null) log.info("Code directory: {}", toCanonical(this.codeDirectory));
    }

    @PostConstruct
    public void init() {
        // Setup the dataset directories.
        try {
            String[] paths = {".", "datasets", "datasets/static", "datasets/base"};
            for (String path : paths) {
                File dir = new File(configurationDir, path);
                if (!dir.mkdirs() && !dir.isDirectory()) {
                    log.error("Failed to create the directory " + dir);
                }
            }
        } catch (Exception e) {
            log.error("Failed to setup", e);
        }
    }

    @Override
    public void onApplicationEvent(ConnectionBuilderChangedEvent event) {
        if (event.type() == ConnectionType.SOURCE) {
            sourceConnectionBuilder = event.connectionBuilder();
        } else if (event.type() == ConnectionType.TARGET) {
            targetConnectionBuilder = event.connectionBuilder();
        }
    }

    public void assertSourceConnection() {
        if (sourceConnectionBuilder == null) throw new HttpStatusException(HttpStatus.BAD_REQUEST, "The source database is not connected");
    }

    public void assertTargetConnection() {
        if (targetConnectionBuilder == null) throw new HttpStatusException(HttpStatus.BAD_REQUEST, "The target database is not connected");
    }

    public boolean hasSourceConnection() {
        return sourceConnectionBuilder != null;
    }

    public Database createSourceDatabase() {
        assertSourceConnection();
        return Database.createDatabase(sourceConnectionBuilder, virtualFkCache);
    }

    public boolean hasTargetConnection() {
        return targetConnectionBuilder != null;
    }

    public Connection createTargetConnection() throws SQLException {
        return targetConnectionBuilder.createConnection();
    }

    public Database createTargetDatabase() {
        assertTargetConnection();
        return Database.createDatabase(targetConnectionBuilder);
    }
}
