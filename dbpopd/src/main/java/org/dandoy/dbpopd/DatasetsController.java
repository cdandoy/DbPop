package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpopd.datasets.FileCacheService;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Controller("/datasets")
@Slf4j
public class DatasetsController {
    private final ConfigurationService configurationService;
    private final FileCacheService fileCacheService;

    public DatasetsController(ConfigurationService configurationService, FileCacheService fileCacheService) {
        this.configurationService = configurationService;
        this.fileCacheService = fileCacheService;
    }

    @Get
    public List<String> getDatasets() {
        return Datasets.getDatasets(configurationService.getDatasetsDirectory())
                .stream().map(Dataset::getName)
                .toList();
    }

    @Get("/files")
    @Deprecated
    public List<DatasetFileRow> getFiles() {
        List<Dataset> datasets = Datasets.getDatasets(configurationService.getDatasetsDirectory())
                .stream()
                .sorted(Comparator.comparing(Dataset::getName))
                .collect(Collectors.toCollection(ArrayList::new));

        List<DatasetFileRow> datasetFileRows = new ArrayList<>();
        for (Dataset dataset : datasets) {
            boolean isFirst = true;
            for (DataFile dataFile : dataset.getDataFiles()) {
                File file = dataFile.getFile();
                FileCacheService.FileInfo fileInfo = fileCacheService.getFileInfo(file);
                datasetFileRows.add(
                        new DatasetFileRow(
                                isFirst ? dataset.getName() : "",
                                dataFile.getTableName().toQualifiedName(),
                                fileInfo.length(),
                                fileInfo.rowCount()
                        )
                );
                isFirst = false;
            }
        }
        return datasetFileRows;
    }


    @Get("/content/{datasetName}")
    public DatasetResponse getDatasetContent(String datasetName) {
        Dataset dataset = Datasets.getDataset(configurationService.getDatasetsDirectory(), datasetName);

        return new DatasetResponse(
                dataset.getName(),
                dataset.getDataFiles().stream()
                        .map(dataFile -> {
                                    File file = dataFile.getFile();
                                    FileCacheService.FileInfo fileInfo = fileCacheService.getFileInfo(file);
                                    return new DatasetDatafileResponse(
                                            file.getName(),
                                            fileInfo.length(),
                                            fileInfo.rowCount()
                                    );
                                }
                        ).toList()
        );
    }

    public record DatasetDatafileResponse(String name, long fileSize, Integer rows) {}

    public record DatasetResponse(String name, List<DatasetDatafileResponse> files) {}

    public record DatasetFileRow(String datasetName, String tableName, Long fileSize, Integer rows) {}
}
