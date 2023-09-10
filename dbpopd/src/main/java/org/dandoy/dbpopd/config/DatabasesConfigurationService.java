package org.dandoy.dbpopd.config;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dandoy.dbpop.database.ConnectionBuilder;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.sql.Connection;
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
    private final ConnectionBuilderChangedEvent[] connectionBuilderChangedEvents = new ConnectionBuilderChangedEvent[2];

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

    public record JdbcUrlComponents(String host, int port) {}

    static JdbcUrlComponents parseJdbcUrl(String url) {
        if (!url.startsWith("jdbc:sqlserver://")) {
            return null;
        }
        url = url.substring("jdbc:sqlserver://".length());
        int i = url.indexOf(';');
        if (i >= 0) {
            url = url.substring(0, i);
        }
        i = url.indexOf(':');
        if (i < 0) {
            return new JdbcUrlComponents(url, 1433);
        }

        String host = url.substring(0, i);
        try {
            int port = Integer.parseInt(url.substring(i + 1));
            return new JdbcUrlComponents(host, port);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static boolean canTcpConnect(String url) {
        JdbcUrlComponents jdbcUrlComponents = parseJdbcUrl(url);
        if (jdbcUrlComponents == null) {
            log.warn("Could not parse the connection string '{}'", url);
            return true; // Assume we can
        }
        log.debug("Attempting a socket connection to {}:{}", jdbcUrlComponents.host(), jdbcUrlComponents.port());
        try (Socket socket = new Socket(jdbcUrlComponents.host(), jdbcUrlComponents.port())) {
            try (InputStream ignored = socket.getInputStream()) {
                return true;
            }
        } catch (Exception ignore) {
            return false;
        }
    }

    @PostConstruct
    public void init() {
        buildConfigs();
    }

    @Scheduled(fixedDelay = "5s", initialDelay = "10s")
    void checkConnections() {
        log.debug("checkConnections()");
        try {
            buildConfigs();
        } catch (Exception e) {
            log.error("Failed to checkConnections()", e);
        }
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
            databaseConfigurationChangedPublisher.publishEvent(
                    new DatabaseConfigurationChangedEvent(
                            type,
                            databaseConfiguration
                    )
            );
            databaseConfigurations[type.ordinal()] = databaseConfiguration;
        }

        // This method is invoked periodically, so we check the database connection even if the configuration has not changed.
        ConnectionBuilderChangedEvent changedEvent = createConnectionBuilderChangedEvent(type, databaseConfiguration);
        if (!Objects.equals(connectionBuilderChangedEvents[type.ordinal()], changedEvent)) {
            connectionBuilderChangedPublisher.publishEvent(changedEvent);
            connectionBuilderChangedEvents[type.ordinal()] = changedEvent;
        }
    }

    private static ConnectionBuilderChangedEvent createConnectionBuilderChangedEvent(ConnectionType type, DatabaseConfiguration databaseConfiguration) {
        if (!databaseConfiguration.hasInfo()) {
            log.debug("Connection {} is not configured", type);
            return new ConnectionBuilderChangedEvent(type, false, null, null);
        }
        if (!canTcpConnect(databaseConfiguration.url())) {
            log.debug("Connection {} - Cannot ping the TCP port - {}", type, databaseConfiguration.url());
            return new ConnectionBuilderChangedEvent(type, true, null, "Database is not running");
        }
        ConnectionBuilder connectionBuilder = databaseConfiguration.createConnectionBuilder();
        try (Connection ignored = connectionBuilder.createConnection()) {
            return new ConnectionBuilderChangedEvent(type, true, connectionBuilder, null);
        } catch (Exception e) {
            log.debug("Failed to connect to " + databaseConfiguration.url(), e);
            return new ConnectionBuilderChangedEvent(type, true, null, e.getMessage());
        }
    }

    public DatabaseConfiguration getDatabaseConfiguration(Properties properties, ConnectionType type) {
        String name = type.name();
        return getDatabaseConfiguration(properties, name + "_JDBCURL", name + "_USERNAME", name + "_PASSWORD");
    }

    private DatabaseConfiguration getDatabaseConfiguration(Properties properties, String urlName, String usernameName, String passwordName) {
        if (!StringUtils.isBlank(System.getenv(urlName))) {
            return new DatabaseConfiguration(
                    System.getenv(urlName),
                    System.getenv(usernameName),
                    System.getenv(passwordName),
                    true
            );
        }

        return new DatabaseConfiguration(
                properties.getProperty(urlName),
                properties.getProperty(usernameName),
                properties.getProperty(passwordName),
                false
        );
    }

    public DatabaseConfiguration getDatabaseConfiguration(ConnectionType type) {
        return databaseConfigurations[type.ordinal()];
    }

    public void setDatabaseConfiguration(ConnectionType type, String url, String username, String password) {
        String name = type.name();
        Properties properties = getConfigurationProperties();
        setProperty(properties, name + "_JDBCURL", url);
        setProperty(properties, name + "_USERNAME", username);
        setProperty(properties, name + "_PASSWORD", password);
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
