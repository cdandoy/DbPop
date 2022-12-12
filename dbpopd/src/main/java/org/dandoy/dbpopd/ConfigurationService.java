package org.dandoy.dbpopd;

import io.micronaut.context.annotation.Property;
import jakarta.inject.Singleton;
import lombok.Getter;
import org.dandoy.dbpop.download.Downloader;
import org.dandoy.dbpop.upload.Populator;
import org.dandoy.dbpop.utils.Env;

import java.io.File;
import java.io.IOException;

@Singleton
public class ConfigurationService {
    @Getter
    private final File configurationDir;
    @Getter
    private final File datasetsDirectory;
    private final Env env;

    public ConfigurationService(
            @SuppressWarnings("MnInjectionPoints") @Property(name = "dbpopd.configuration.path") String configurationPath
    ) {
        configurationDir = toConfigurationDir(configurationPath);
        datasetsDirectory = new File(configurationDir, "datasets");
        // TODO: This is so wrong. I need to cleanup the configuration classes
        env = Env.createEnv(configurationDir);
        if (env == null) {
            throw new RuntimeException("Cannot load the configuration from " + new File(configurationDir, "dbpop.properties"));
        }
    }

    private static File toConfigurationDir(String configurationPath) {
        try {
            File configurationDir = new File(configurationPath).getCanonicalFile();
            File configurationFile = new File(configurationDir, "dbpop.properties");
            if (!configurationFile.canRead()) {
                throw new RuntimeException("Cannot read the configuration file: %s".formatted(configurationFile));
            }
            return configurationDir;
        } catch (IOException e) {
            throw new RuntimeException("Invalid configuration path: %s".formatted(configurationPath), e);
        }
    }

    public Populator createPopulator() {
        return Populator.builder()
                .setDbUrl(env.getString("jdbcurl"))
                .setDbUser(env.getString("username"))
                .setDbPassword(env.getString("password"))
                .setDirectory(datasetsDirectory)
                .build();
    }

    public Downloader.Builder createDownloadBuilder() {
        return Downloader.builder()
                .setDirectory(datasetsDirectory);
    }
}
