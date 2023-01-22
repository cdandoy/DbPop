package org.dandoy.dbpopd;

import io.micronaut.context.annotation.Property;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Singleton;
import lombok.Getter;
import org.dandoy.dbpop.database.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

@SuppressWarnings("unused")
@Singleton
public class ConfigurationService {
    private static final String PROP_FILE_NAME = "dbpop.properties";
    @Getter
    private final File configurationDir;
    @Getter
    private final ConnectionBuilder sourceConnectionBuilder;
    @Getter
    private final ConnectionBuilder targetConnectionBuilder;
    private final File datasetsDirectory;
    private DatabaseCache sourceDatabaseCache;
    private DatabaseCache targetDatabaseCache;
    @Getter
    private final VirtualFkCache virtualFkCache;

    @SuppressWarnings("MnInjectionPoints")
    public ConfigurationService(
            @Property(name = "dbpopd.configuration.path") String configurationPath,
            @Property(name = "dbpopd.configuration.datasets") @Nullable String datasetsDirectory
    ) {
        configurationDir = new File(configurationPath);
        File configurationFile = new File(configurationDir, PROP_FILE_NAME);
        Properties properties = getConfigurationProperties(configurationFile);

        sourceConnectionBuilder = createConnectionBuilder(properties, "SOURCE_JDBCURL", "SOURCE_USERNAME", "SOURCE_PASSWORD");
        targetConnectionBuilder = createConnectionBuilder(properties, "TARGET_JDBCURL", "TARGET_USERNAME", "TARGET_PASSWORD");

        File vfkFile = new File(configurationDir, "vfk.json");
        virtualFkCache = VirtualFkCache.createVirtualFkCache(vfkFile);
        this.datasetsDirectory = datasetsDirectory != null ? new File(datasetsDirectory) : new File(configurationDir, "datasets");
    }

    private static UrlConnectionBuilder createConnectionBuilder(Properties properties, String jdbcurlProperty, String usernameProperty, String passwordProperty) {
        String url = getProperty(properties, jdbcurlProperty);
        String username = getProperty(properties, usernameProperty);
        String password = getProperty(properties, passwordProperty);

        if (url == null || username == null) return null;

        return new UrlConnectionBuilder(url, username, password);
    }

    public File getDatasetsDirectory() {
        return datasetsDirectory;
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
        return sourceDatabaseCache;
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
        return targetDatabaseCache;
    }
}
