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
import java.util.Objects;
import java.util.Properties;

import static org.dandoy.dbpopd.utils.IOUtils.toCanonical;

@Singleton
@Context
@Slf4j
public class DatabasesConfigurationService {
    private final ApplicationEventPublisher<DatabaseConfigurationChangedEvent> databaseConfigurationChangedPublisher;
    private final File configurationFile;
    private final ApplicationEventPublisher<ConnectionBuilderChangedEvent> connectionBuilderChangedPublisher;
    private final DatabaseConfiguration[] databaseConfigurations = new DatabaseConfiguration[2];
    private final ConnectionBuilder[] connectionBuilders = new ConnectionBuilder[2];

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
        // Deal with configurations
        Properties properties = getConfigurationProperties(configurationFile);
        buildDatabaseConfig(properties, ConnectionType.SOURCE);
        buildDatabaseConfig(properties, ConnectionType.TARGET);
    }

    private void buildDatabaseConfig(Properties properties, ConnectionType type) {
        DatabaseConfiguration databaseConfiguration = getDatabaseConfiguration(properties, type);
        if (!Objects.equals(databaseConfigurations[type.ordinal()], databaseConfiguration)) {
            databaseConfigurations[type.ordinal()] = databaseConfiguration;
            databaseConfigurationChangedPublisher.publishEvent(
                    new DatabaseConfigurationChangedEvent(
                            type,
                            databaseConfiguration
                    )
            );

            ConnectionBuilder connectionBuilder = databaseConfiguration.createConnectionBuilder();
            ConnectionBuilder currentConnectionBuilder = connectionBuilders[type.ordinal()];
            if (!(connectionBuilder == null && currentConnectionBuilder == null)) { // Don't fire an event if they are both null
                connectionBuilderChangedPublisher.publishEvent(
                        new ConnectionBuilderChangedEvent(
                                type,
                                connectionBuilder
                        )
                );
                connectionBuilders[type.ordinal()] = connectionBuilder;
            }
        }
    }

    public DatabaseConfiguration getDatabaseConfiguration(Properties properties, ConnectionType type) {
        String name = type.name();
        return getDatabaseConfiguration(properties, name + "_JDBCURL", name + "_USERNAME", name + "_PASSWORD");
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
        return getDatabaseConfiguration(properties, ConnectionType.SOURCE).createConnectionBuilder();
    }

    /**
     * For tests only
     */
    public ConnectionBuilder _createTargetConnectionBuilder() {
        Properties properties = getConfigurationProperties(configurationFile);
        return getDatabaseConfiguration(properties, ConnectionType.TARGET).createConnectionBuilder();
    }
}
