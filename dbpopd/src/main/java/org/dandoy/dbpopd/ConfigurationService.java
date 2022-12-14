package org.dandoy.dbpopd;

import io.micronaut.context.annotation.Property;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.SneakyThrows;
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
        File configurationFile = getConfigurationFile(configurationPath);
        Properties properties = getConfigurationProperties(configurationFile);
        jdbcurl = getValidProperty(configurationFile, properties, "jdbcurl");
        username = getValidProperty(configurationFile, properties, "username");
        password = getValidProperty(configurationFile, properties, "password");
        File configurationDir = configurationFile.getParentFile();
        datasetsDirectory = new File(configurationDir, "datasets");
    }

    private static String getValidProperty(File configurationFile, Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null) throw new RuntimeException("Missing property %s in %s".formatted(name, configurationFile));
        return value;
    }

    @SneakyThrows
    private static Properties getConfigurationProperties(File configurationFile) {
        Properties properties = new Properties();
        try (BufferedReader bufferedReader = Files.newBufferedReader(configurationFile.toPath())) {
            properties.load(bufferedReader);
        }
        return properties;
    }

    private static File getConfigurationFile(String configurationPath) {
        try {
            File configurationDir = new File(configurationPath).getCanonicalFile();
            File configurationFile = new File(configurationDir, PROP_FILE_NAME);
            if (!configurationFile.canRead()) {
                throw new RuntimeException("Cannot read the configuration file: %s".formatted(configurationFile));
            }
            return configurationFile;
        } catch (IOException e) {
            throw new RuntimeException("Invalid configuration path: %s".formatted(configurationPath), e);
        }
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
