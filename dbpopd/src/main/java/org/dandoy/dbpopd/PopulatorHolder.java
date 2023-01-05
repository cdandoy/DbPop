package org.dandoy.dbpopd;

import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.upload.Populator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Holds onto a Populator until one of the static dataset files gets updated.
 * The goal is to load the static dataset once unless the data has changed.
 */
@Singleton
@Slf4j
public class PopulatorHolder {
    private final ConfigurationService configurationService;
    private Populator populator;
    private long lastTimestamp;

    public PopulatorHolder(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @PreDestroy
    public void preDestroy() {
        closePopulator();
    }

    public Populator getPopulator() {
        if (populator == null) {
            createPopulator();
        } else if (hasUpdatedFiles()) {
            createPopulator();
        }
        return populator;
    }

    private boolean hasUpdatedFiles() {
        File datasetsDirectory = configurationService.getDatasetsDirectory();
        File staticDirectory = new File(datasetsDirectory, "static");
        return lastTimestamp < getTimestamp(staticDirectory);
    }

    private long getTimestamp(File file) {
        try (Stream<Path> pathStream = Files.walk(file.toPath())) {
            return pathStream
                    .map(path -> path.toFile().lastModified())
                    .max(Long::compareTo)
                    .orElse(0L);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get the timestamps on " + file, e);
        }
    }

    private void closePopulator() {
        if (populator != null) {
            populator.close();
        }
    }

    private void createPopulator() {
        configurationService.assertTargetConnection();
        closePopulator();
        populator = Populator.createPopulator(configurationService.createTargetDatabase(), configurationService.getDatasetsDirectory());
        lastTimestamp = System.currentTimeMillis();
    }

    public void reset() {
        populator = null;
    }
}
