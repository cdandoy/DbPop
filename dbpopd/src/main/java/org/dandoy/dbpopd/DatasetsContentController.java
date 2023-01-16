package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpopd.datasets.FileCacheService;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dandoy.dbpop.datasets.Datasets.DATASET_NAME_COMPARATOR;

@Controller
@Slf4j
public class DatasetsContentController {
    private final ConfigurationService configurationService;
    private final FileCacheService fileCacheService;

    public DatasetsContentController(ConfigurationService configurationService, FileCacheService fileCacheService) {
        this.configurationService = configurationService;
        this.fileCacheService = fileCacheService;
    }

    @Get("/datasets/content")
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
                        .sorted((o1, o2) -> DATASET_NAME_COMPARATOR.compare(o1.name, o2.name))
                        .toList(),
                datasetContentTables.entrySet()
                        .stream().map(it -> new TableContent(
                                it.getKey(),
                                it.getValue()
                        ))
                        .sorted(Comparator.comparing(it -> it.tableName.toQualifiedName()))
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
        return new DatasetContent(
                dataset.getName(),
                dataset.getDataFiles().size(),
                size,
                rows
        );
    }

    public record DatasetContentResponse(List<DatasetContent> datasetContents, List<TableContent> tableContents) {}

    public record DatasetContent(String name, int fileCount, long size, int rows) {}

    public record TableContent(TableName tableName, Map<String, FileContent> content) {}

    public record FileContent(long size, Integer rows) {}
}
