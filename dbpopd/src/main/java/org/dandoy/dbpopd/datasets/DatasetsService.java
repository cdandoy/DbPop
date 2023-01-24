package org.dandoy.dbpopd.datasets;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpopd.ConfigurationService;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dandoy.dbpop.datasets.Datasets.DATASET_NAME_COMPARATOR;

@Singleton
@Slf4j
public class DatasetsService {
    private final ConfigurationService configurationService;
    private final FileCacheService fileCacheService;
    private Status lastStatus = Status.NOT;

    private record Status(String datasetName, List<String> failedDatasetCauses, Integer lastRows, Long lastTime) {
        static Status NOT = new Status("", null, null, null);

        public Status(String datasetName, Integer lastRows, Long lastTime) {
            this(datasetName, null, lastRows, lastTime);
        }

        public Status(String datasetName, List<String> failedDatasetCauses) {
            this(datasetName, failedDatasetCauses, null, null);
        }
    }

    public DatasetsService(ConfigurationService configurationService, FileCacheService fileCacheService) {
        this.configurationService = configurationService;
        this.fileCacheService = fileCacheService;
    }

    public List<String> getDatasets() {
        return Datasets.getDatasets(configurationService.getDatasetsDirectory())
                .stream().map(Dataset::getName)
                .toList();
    }

    public DatasetContentResponse getContent() {
        File datasetsDirectory = configurationService.getDatasetsDirectory();
        Map<TableName, Map<String, FileContent>> datasetContentTables = new HashMap<>();
        List<Dataset> datasets = Datasets.getDatasets(datasetsDirectory);
        for (Dataset dataset : datasets) {
            String datasetName = dataset.getName();
            for (DataFile dataFile : dataset.getDataFiles()) {
                TableName tableName = dataFile.getTableName();
                File file = dataFile.getFile();
                FileCacheService.FileInfo fileInfo = fileCacheService.getFileInfo(file);
                datasetContentTables.computeIfAbsent(tableName, tableName1 -> new HashMap<>())
                        .put(datasetName,
                                new FileContent(
                                        fileInfo.length(),
                                        fileInfo.rowCount()
                                )
                        );
            }
        }
        return new DatasetContentResponse(
                datasets.stream()
                        .map(this::toDatasetContent)
                        .sorted((o1, o2) -> DATASET_NAME_COMPARATOR.compare(o1.name(), o2.name()))
                        .toList(),
                datasetContentTables.entrySet()
                        .stream().map(it -> new TableContent(
                                it.getKey(),
                                it.getValue()
                        ))
                        .sorted(Comparator.comparing(it -> it.tableName().toQualifiedName()))
                        .toList()
        );
    }

    private DatasetContent toDatasetContent(Dataset dataset) {
        long size = 0;
        int rows = 0;
        for (DataFile dataFile : dataset.getDataFiles()) {
            File file = dataFile.getFile();
            FileCacheService.FileInfo fileInfo = fileCacheService.getFileInfo(file);
            size += fileInfo.length();
            Integer csvRowCount = fileInfo.rowCount();
            if (csvRowCount != null) {
                rows += csvRowCount;
            }
        }
        String datasetName = dataset.getName();
        boolean active = datasetName.equals(lastStatus.datasetName);
        Status status = active ? lastStatus : Status.NOT;
        return new DatasetContent(datasetName, dataset.getDataFiles().size(), size, rows, active, status.lastRows(), status.lastTime(), status.failedDatasetCauses());
    }

    public void setFailedDataset(String dataset, List<String> causes) {
        lastStatus = new Status(dataset, causes);
    }

    public void setActive(String datasetName, int rows, long time) {
        lastStatus = new Status(datasetName, rows, time);
    }

}
