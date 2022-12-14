package org.dandoy.dbpopd;

import io.micronaut.context.annotation.Property;
import jakarta.inject.Singleton;
import lombok.Getter;
import org.dandoy.dbpop.download.Downloader;
import org.dandoy.dbpop.upload.Populator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

@Singleton
public class ConfigurationService {
    private static final String PROP_FILE_NAME = "dbpop.properties";
    @Getter
    private final File datasetsDirectory;
    private final String jdbcurl;
    private final String username;
    private final String password;

    public ConfigurationService(
            @SuppressWarnings("MnInjectionPoints") @Property(name = "dbpopd.configuration.path") String configurationPath
    ) {
        File configurationDir = new File(configurationPath);
        File configurationFile = new File(configurationDir, PROP_FILE_NAME);
        Properties properties = getConfigurationProperties(configurationFile);
        jdbcurl = getValidProperty(configurationFile, properties, "jdbcurl", "DBPOP_JDBCURL");
        username = getValidProperty(configurationFile, properties, "username", "DBPOP_USERNAME");
        password = getValidProperty(configurationFile, properties, "password", "DBPOP_PASSWORD");
        datasetsDirectory = new File(configurationDir, "datasets");
    }

    private static String getValidProperty(File configurationFile, Properties properties, String propertyName, String environmentVariableName) {
        String environmentValue = System.getenv(environmentVariableName);
        if (environmentValue != null) return environmentValue;

        String value = properties.getProperty(propertyName);
        if (value != null) return value;

        throw new RuntimeException("Missing environment variable %s or property %s in %s".formatted(environmentVariableName, propertyName, configurationFile));
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

    public Populator createPopulator() {
        return Populator.builder()
                .setDbUrl(jdbcurl)
                .setDbUser(username)
                .setDbPassword(password)
                .setDirectory(datasetsDirectory)
                .build();
    }

    public Downloader.Builder createDownloadBuilder() {
        return Downloader.builder()
                .setDirectory(datasetsDirectory);
    }
}
