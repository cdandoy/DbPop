package org.dandoy.dbpopd.populate;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.DatabaseCache;
import org.dandoy.dbpop.upload.Populator;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.utils.FileUtils;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
@Slf4j
public class PopulateService {
    private final ConfigurationService configurationService;
    private final Map<File, Long> fileTimestamps = new HashMap<>();

    public PopulateService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public PopulateResult populate(List<String> dataset) {
        long t0 = System.currentTimeMillis();
        DatabaseCache databaseCache = configurationService.getTargetDatabaseCache();
        Populator populator = Populator.createPopulator(databaseCache, configurationService.getDatasetsDirectory());
        boolean staticChanged = hasStaticChanged();
        populator.setStaticLoaded(!staticChanged);
        int rows = populator.load(dataset);
        long t1 = System.currentTimeMillis();
        return new PopulateResult(rows, t1 - t0);
    }

    private boolean hasStaticChanged() {
        boolean ret = false;
        File datasetsDirectory = configurationService.getDatasetsDirectory();
        File staticDir = new File(datasetsDirectory, "static");
        if (staticDir.isDirectory()) {
            List<File> files = FileUtils.getFiles(staticDir);
            Map<File, Long> newMap = new HashMap<>();
            for (File file : files) {
                Long lastModified = fileTimestamps.remove(file);
                long thatLastModified = file.lastModified();
                if (lastModified == null) {
                    ret = true; // new file
                } else if (lastModified != thatLastModified) {
                    ret = true; // different timestamp
                }
                newMap.put(file, thatLastModified);
            }
            if (!fileTimestamps.isEmpty()) {
                ret = true;
                fileTimestamps.clear();
            }
            fileTimestamps.putAll(newMap);
        }
        return ret;
    }

    public record PopulateResult(int rows, long millis) {}
}
