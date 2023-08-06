package org.dandoy.dbpopd.populate;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.DatabaseCache;
import org.dandoy.dbpop.upload.PopulateDatasetException;
import org.dandoy.dbpop.upload.Populator;
import org.dandoy.dbpop.upload.PopulatorListener;
import org.dandoy.dbpop.utils.ExceptionUtils;
import org.dandoy.dbpop.utils.MultiCauseException;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.DatabaseCacheService;
import org.dandoy.dbpopd.datasets.DatasetsService;
import org.dandoy.dbpopd.extensions.ExtensionService;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Singleton
@Slf4j
public class PopulateService {
    private final ConfigurationService configurationService;
    private final DatabaseCacheService databaseCacheService;
    private final DatasetsService datasetsService;
    private final ExtensionService extensionService;
    private Map<File, Long> fileTimestamps = new HashMap<>();
    private final PopulatorListener populatorListener = new PopulatorListener() {
        @Override
        public void afterPopulate() {
            extensionService.afterPopulate();
        }

        @Override
        public void afterPopulate(String dataset) {
            extensionService.afterPopulate(dataset);
        }
    };

    public PopulateService(ConfigurationService configurationService, DatabaseCacheService databaseCacheService, DatasetsService datasetsService, ExtensionService extensionService) {
        this.configurationService = configurationService;
        this.databaseCacheService = databaseCacheService;
        this.datasetsService = datasetsService;
        this.extensionService = extensionService;
    }

    public PopulateResult populate(List<String> dataset) {
        return populate(dataset, false);
    }

    public PopulateResult populate(List<String> datasets, boolean forceStatic) {
        if (datasets.isEmpty()) throw new HttpStatusException(HttpStatus.BAD_REQUEST, "No datasets to download");
        try {
            long t0 = System.currentTimeMillis();
            DatabaseCache databaseCache = databaseCacheService.getTargetDatabaseCache();
            Populator populator = Populator
                    .createPopulator(databaseCache, configurationService.getDatasetsDirectory())
                    .setPopulatorListener(populatorListener);
            if (forceStatic) {
                populator.setStaticLoaded(false);
            } else {
                boolean staticChanged = hasStaticChanged();
                populator.setStaticLoaded(!staticChanged);
            }
            int rows = populator.load(datasets);
            long t1 = System.currentTimeMillis();
            datasetsService.setActive(datasets.get(datasets.size() - 1), rows, t1 - t0);

            captureStaticTimestamps();

            return new PopulateResult(rows, t1 - t0);
        } catch (Exception e) {
            Optional<PopulateDatasetException> optionalCause = ExceptionUtils.getCause(e, PopulateDatasetException.class);
            if (optionalCause.isPresent()) {
                PopulateDatasetException populateDatasetException = optionalCause.get();
                String dataset = populateDatasetException.getDataset();
                List<String> causes = MultiCauseException.getCauses(populateDatasetException);
                datasetsService.setFailedDataset(dataset, causes);
            } else { // Not sure which dataset failed to load
                List<String> causes = MultiCauseException.getCauses(e);
                datasetsService.setFailedDataset(datasets.get(0), causes);
            }
            throw e;
        }
    }

    private boolean hasStaticChanged() {
        boolean ret = false;
        File datasetsDirectory = configurationService.getDatasetsDirectory();
        File staticDir = new File(datasetsDirectory, "static");
        if (staticDir.isDirectory()) {
            List<File> files = DbPopdFileUtils.getFiles(staticDir);
            for (File file : files) {
                Long lastModified = fileTimestamps.get(file);
                long thatLastModified = file.lastModified();
                if (lastModified == null) {
                    ret = true; // new file
                } else if (lastModified != thatLastModified) {
                    ret = true; // different timestamp
                }
            }
        }
        return ret;
    }

    private void captureStaticTimestamps() {
        File datasetsDirectory = configurationService.getDatasetsDirectory();
        File staticDir = new File(datasetsDirectory, "static");
        if (staticDir.isDirectory()) {
            List<File> files = DbPopdFileUtils.getFiles(staticDir);
            Map<File, Long> newMap = new HashMap<>();
            for (File file : files) {
                long lastModified = file.lastModified();
                newMap.put(file, lastModified);
            }
            fileTimestamps = newMap;
        }
    }
}
