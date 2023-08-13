package org.dandoy.dbpopd.config;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.event.ApplicationEventPublisher;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dandoy.dbpop.database.ConnectionBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
    @Getter
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

    private Properties getConfigurationProperties() {
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

    private void saveConfigurationProperties(Properties properties) {
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(configurationFile.toPath())) {
            properties.store(bufferedWriter, null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write " + configurationFile, e);
        }
        buildConfigs();
    }

    private void buildConfigs() {
        // Deal with configurations
        Properties properties = getConfigurationProperties();
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

    public DatabaseConfiguration getDatabaseConfiguration(ConnectionType type) {
        return databaseConfigurations[type.ordinal()];
    }

    public void setDatabaseConfiguration(ConnectionType type, DatabaseConfiguration configuration) {
        String name = type.name();
        Properties properties = getConfigurationProperties();
        setProperty(properties, name + "_JDBCURL", configuration.url());
        setProperty(properties, name + "_USERNAME", configuration.username());
        setProperty(properties, name + "_PASSWORD", configuration.password());
        saveConfigurationProperties(properties);
    }

    private static void setProperty(Properties properties, String name, String value) {
        if (StringUtils.isBlank(value)) {
            properties.remove(name);
        } else {
            properties.setProperty(name, value);
        }
    }

    /**
     * For tests only
     */
    public ConnectionBuilder _createSourceConnectionBuilder() {
        Properties properties = getConfigurationProperties();
        return getDatabaseConfiguration(properties, ConnectionType.SOURCE).createConnectionBuilder();
    }

    /**
     * For tests only
     */
    public ConnectionBuilder _createTargetConnectionBuilder() {
        Properties properties = getConfigurationProperties();
        return getDatabaseConfiguration(properties, ConnectionType.TARGET).createConnectionBuilder();
    }
}
