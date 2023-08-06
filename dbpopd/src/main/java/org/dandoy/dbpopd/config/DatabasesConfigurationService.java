package org.dandoy.dbpopd.config;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.event.ApplicationEventPublisher;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ConnectionBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

import static org.dandoy.dbpopd.utils.IOUtils.toCanonical;

@Singleton
@Context
@Slf4j
public class DatabasesConfigurationService {
    private final ApplicationEventPublisher<DatabaseConfigurationChangedEvent> databaseConfigurationChangedPublisher;
    private final File configurationFile;
    private final ApplicationEventPublisher<ConnectionBuilderChangedEvent> connectionBuilderChangedPublisher;

    public DatabasesConfigurationService(
            @SuppressWarnings("MnInjectionPoints") @Property(name = "dbpopd.configuration.path") String configurationPath,
            ApplicationEventPublisher<DatabaseConfigurationChangedEvent> databaseConfigurationChangedPublisher,
            ApplicationEventPublisher<ConnectionBuilderChangedEvent> connectionBuilderChangedPublisher
    ) {
        this.connectionBuilderChangedPublisher = connectionBuilderChangedPublisher;
        File configurationDir = toCanonical(new File(configurationPath));
        configurationFile = new File(configurationDir, ConfigurationService.PROP_FILE_NAME);
        this.databaseConfigurationChangedPublisher = databaseConfigurationChangedPublisher;
    }

    @PostConstruct
    public void init() {
        buildConfigs();
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

    private void buildConfigs() {
        Properties properties = getConfigurationProperties(configurationFile);
        DatabaseConfiguration sourceDatabaseConfiguration = getSourceDatabaseConfiguration(properties);
        databaseConfigurationChangedPublisher.publishEvent(
                new DatabaseConfigurationChangedEvent(
                        ConnectionType.SOURCE,
                        sourceDatabaseConfiguration
                )
        );

        DatabaseConfiguration targetDatabaseConfiguration = getTargetDatabaseConfiguration(properties);
        databaseConfigurationChangedPublisher.publishEvent(
                new DatabaseConfigurationChangedEvent(
                        ConnectionType.TARGET,
                        targetDatabaseConfiguration
                )
        );

        connectionBuilderChangedPublisher.publishEvent(
                new ConnectionBuilderChangedEvent(
                        ConnectionType.SOURCE,
                        sourceDatabaseConfiguration.createConnectionBuilder()
                )
        );
        connectionBuilderChangedPublisher.publishEvent(
                new ConnectionBuilderChangedEvent(
                        ConnectionType.TARGET,
                        targetDatabaseConfiguration.createConnectionBuilder()
                )
        );
    }

    public DatabaseConfiguration getTargetDatabaseConfiguration(Properties properties) {
        return getDatabaseConfiguration(properties, "TARGET_JDBCURL", "TARGET_USERNAME", "TARGET_PASSWORD");
    }

    public DatabaseConfiguration getSourceDatabaseConfiguration(Properties properties) {
        return getDatabaseConfiguration(properties, "SOURCE_JDBCURL", "SOURCE_USERNAME", "SOURCE_PASSWORD");
    }

    private DatabaseConfiguration getDatabaseConfiguration(Properties properties, String urlName, String usernameName, String passwordName) {
        DatabaseConfiguration envSource = new DatabaseConfiguration(
                System.getenv(urlName),
                System.getenv(usernameName),
                System.getenv(passwordName)
        );
        DatabaseConfiguration fileSource = new DatabaseConfiguration(
                properties.getProperty(urlName),
                properties.getProperty(usernameName),
                properties.getProperty(passwordName)
        );
        return new DatabaseConfiguration(
                fileSource.hasInfo() ? fileSource : envSource,
                envSource.hasInfo() && fileSource.hasInfo()
        );
    }

    /**
     * For tests only
     */
    public ConnectionBuilder _createSourceConnectionBuilder() {
        Properties properties = getConfigurationProperties(configurationFile);
        return getSourceDatabaseConfiguration(properties).createConnectionBuilder();
    }
    /**
     * For tests only
     */
    public ConnectionBuilder _createTargetConnectionBuilder() {
        Properties properties = getConfigurationProperties(configurationFile);
        return getTargetDatabaseConfiguration(properties).createConnectionBuilder();
    }
}
