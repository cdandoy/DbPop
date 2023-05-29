package org.dandoy.dbpopd;

import io.micronaut.context.annotation.Property;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.dandoy.dbpopd.utils.IOUtils.toCanonical;

@SuppressWarnings("unused")
@Singleton
@Slf4j
public class ConfigurationService {
    private static final String PROP_FILE_NAME = "dbpop.properties";
    @Getter
    private final File configurationDir;
    @Getter
    private final ConnectionBuilder sourceConnectionBuilder;
    @Getter
    private final ConnectionBuilder targetConnectionBuilder;
    @Getter
    private final File datasetsDirectory;
    @Getter
    private final File setupDirectory;
    @Getter
    private final File codeDirectory;
    @Getter
    private final File extensionsDirectory;
    @Getter
    private final File snapshotFile;
    @Getter
    private final File flywayDirectory;
    private DatabaseCache sourceDatabaseCache;
    private DatabaseCache targetDatabaseCache;
    @Getter
    private final VirtualFkCache virtualFkCache;
    @Getter
    @Setter
    private boolean codeAutoSave;

    @SuppressWarnings("MnInjectionPoints")
    public ConfigurationService(
            @Property(name = "dbpopd.configuration.path") String configurationPath,
            @Property(name = "dbpopd.configuration.datasets") @Nullable String datasetsDirectory,
            @Property(name = "dbpopd.configuration.setup") @Nullable String setupDirectory,
            @Property(name = "dbpopd.configuration.code") @Nullable String codeDirectory,
            @Property(name = "dbpopd.configuration.extensions") @Nullable String extensionsDirectory,
            @Property(name = "dbpopd.configuration.snapshot") @Nullable String snapshotFile,
            @Property(name = "dbpopd.configuration.flyway.path") @Nullable String flywayDirectory,
            @Property(name = "dbpopd.configuration.codeAutoSave", defaultValue = "false") boolean codeAutoSave
    ) {
        configurationDir = toCanonical(new File(configurationPath));
        File configurationFile = new File(configurationDir, PROP_FILE_NAME);
        Properties properties = getConfigurationProperties(configurationFile);

        sourceConnectionBuilder = createConnectionBuilder(properties, "SOURCE_JDBCURL", "SOURCE_USERNAME", "SOURCE_PASSWORD");
        targetConnectionBuilder = createConnectionBuilder(properties, "TARGET_JDBCURL", "TARGET_USERNAME", "TARGET_PASSWORD");

        File vfkFile = new File(configurationDir, "vfk.json");
        virtualFkCache = VirtualFkCache.createVirtualFkCache(vfkFile);
        this.datasetsDirectory = datasetsDirectory != null ? toCanonical(new File(datasetsDirectory)) : new File(configurationDir, "datasets");
        this.setupDirectory = setupDirectory != null ? toCanonical(new File(setupDirectory)) : new File(configurationDir, "setup");
        this.codeDirectory = codeDirectory != null ? toCanonical(new File(codeDirectory)) : new File(configurationDir, "code");
        this.extensionsDirectory = extensionsDirectory != null ? toCanonical(new File(extensionsDirectory)) : new File(configurationDir, "extensions");
        this.snapshotFile = snapshotFile != null ? toCanonical(new File(snapshotFile)) : new File(configurationDir, "snapshot");
        this.flywayDirectory = flywayDirectory != null ? toCanonical(new File(flywayDirectory)) : new File(configurationDir, "flyway");
        this.codeAutoSave = codeAutoSave;

        log.info("Configuration directory: {}", toCanonical(this.configurationDir));
        if (datasetsDirectory != null) log.info("Datasets directory: {}", toCanonical(this.datasetsDirectory));
        if (setupDirectory != null) log.info("Setup directory: {}", toCanonical(this.setupDirectory));
        if (codeDirectory != null) log.info("Code directory: {}", toCanonical(this.codeDirectory));
    }

    private static UrlConnectionBuilder createConnectionBuilder(Properties properties, String jdbcurlProperty, String usernameProperty, String passwordProperty) {
        String url = getProperty(properties, jdbcurlProperty);
        String username = getProperty(properties, usernameProperty);
        String password = getProperty(properties, passwordProperty);

        if (url == null || username == null) return null;

        return new UrlConnectionBuilder(url, username, password);
    }

    private static String getProperty(Properties properties, String propertyName) {
        String environmentValue = System.getenv(propertyName);
        if (environmentValue != null) return environmentValue;

        return properties.getProperty(propertyName);
    }

    private static Properties getConfigurationProperties(File configurationFile) {
        Properties properties = new Properties();
        if (configurationFile.canRead()) {
            try (BufferedReader bufferedReader = Files.newBufferedReader(configurationFile.toPath())) {
                properties.load(bufferedReader);
            } catch (IOException e) {
                throw new RuntimeException("Cannot read " + configurationFile, e);
            }
        }
        return properties;
    }

    public void assertSourceConnection() {
        if (sourceConnectionBuilder == null) throw new HttpStatusException(HttpStatus.BAD_REQUEST, "The source database has not been defined");
    }

    public void assertTargetConnection() {
        if (targetConnectionBuilder == null) throw new HttpStatusException(HttpStatus.BAD_REQUEST, "The target database has not been defined");
    }

    public boolean hasSourceConnection() {
        return sourceConnectionBuilder != null;
    }

    public Database createSourceDatabase() {
        return Database.createDatabase(sourceConnectionBuilder, virtualFkCache);
    }

    public boolean hasTargetConnection() {
        return targetConnectionBuilder != null;
    }

    public Connection createTargetConnection() throws SQLException {
        return targetConnectionBuilder.createConnection();
    }

    public Database createTargetDatabase() {
        return Database.createDatabase(targetConnectionBuilder);
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
