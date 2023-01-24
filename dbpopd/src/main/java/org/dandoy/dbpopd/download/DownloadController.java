package org.dandoy.dbpopd.download;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Dependency;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.download.*;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.populate.PopulateService;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

@Controller("/download")
public class DownloadController {
    private final ConfigurationService configurationService;
    private final PopulateService populateService;

    public DownloadController(ConfigurationService configurationService, PopulateService populateService) {
        this.configurationService = configurationService;
        this.populateService = populateService;
    }

    @Post("/model")
    public DownloadResponse download(@Body DownloadRequest downloadRequest) {
        configurationService.assertSourceConnection();

        ExecutionMode executionMode = downloadRequest.isDryRun() ? ExecutionMode.COUNT : ExecutionMode.SAVE;
        // TableExecutionModel is the recursive description at the TableName level
        TableExecutionModel tableExecutionModel = toTableExecutionModel(downloadRequest.getDependency());

        List<String> filteredColumns = new ArrayList<>();
        Set<List<Object>> pks = new HashSet<>();
        Map<String, String> queryValues = downloadRequest.getQueryValues();
        if (queryValues != null && !queryValues.isEmpty()) {
            List<Object> pk = new ArrayList<>();
            for (Map.Entry<String, String> entry : queryValues.entrySet()) {
                String column = entry.getKey();
                filteredColumns.add(column);
                String value = entry.getValue();
                pk.add(value);
            }
            pks.add(pk);
        }

        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            // ExecutionPlan is the recursive description at the Table level
            ExecutionContext executionContext = ExecutionPlan.execute(
                    sourceDatabase,
                    configurationService.getDatasetsDirectory(),
                    downloadRequest.getDataset(),
                    downloadRequest.getDependency().getTableName(),
                    tableExecutionModel,
                    filteredColumns,
                    pks,
                    executionMode,
                    downloadRequest.getMaxRows()
            );
            if (!downloadRequest.isDryRun()) {
                populateService.populate(List.of(downloadRequest.getDataset()));
            }
            return new DownloadResponse(
                    executionContext.getRowCounts(),
                    executionContext.getRowsSkipped(),
                    !executionContext.keepRunning()
            );
        }
    }

    @Post("/bulk")
    public DownloadResponse downloadBulk(@Body DownloadBulkBody downloadBulkBody) {
        ExecutionContext executionContext = new ExecutionContext();
        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            for (TableName tableName : downloadBulkBody.tableNames) {
                try (TableDownloader tableDownloader = TableDownloader.builder()
                        .setDatabase(sourceDatabase)
                        .setDatasetsDirectory(configurationService.getDatasetsDirectory())
                        .setDataset(downloadBulkBody.dataset())
                        .setTableName(tableName)
                        .setExecutionMode(ExecutionMode.SAVE)
                        .setExecutionContext(executionContext)
                        .build()) {
                    executionContext.tableAdded(tableName);
                    tableDownloader.download();
                }
            }
        }
        populateService.populate(List.of(downloadBulkBody.dataset));
        return new DownloadResponse(
                executionContext.getRowCounts(),
                executionContext.getRowsSkipped(),
                !executionContext.keepRunning()
        );
    }

    @Post("/target")
    public DownloadResponse downloadTarget(@Body DownloadTargetBody downloadTargetBody) {
        File datasetsDirectory = configurationService.getDatasetsDirectory();

        List<String> datasetNames;
        if (Datasets.STATIC.equals(downloadTargetBody.dataset())) datasetNames = List.of(Datasets.STATIC);
        else if (Datasets.BASE.equals(downloadTargetBody.dataset())) datasetNames = List.of(Datasets.STATIC, Datasets.BASE);
        else datasetNames = List.of(Datasets.STATIC, Datasets.BASE, downloadTargetBody.dataset());

        ExecutionContext executionContext = new ExecutionContext();
        try (Database targetDatabase = configurationService.createTargetDatabase()) {
            for (String datasetName : datasetNames) {
                Dataset dataset = Datasets.getDataset(configurationService.getDatasetsDirectory(), datasetName);
                for (DataFile dataFile : dataset.getDataFiles()) {
                    TableName tableName = dataFile.getTableName();
                    File file = dataFile.getFile();
                    if (!file.delete() && file.exists()) throw new RuntimeException("Failed to replace " + file);
                    try (TableDownloader tableDownloader = TableDownloader.builder()
                            .setDatabase(targetDatabase)
                            .setDatasetsDirectory(datasetsDirectory)
                            .setDataset(datasetName)
                            .setTableName(tableName)
                            .setExecutionMode(ExecutionMode.SAVE)
                            .setExecutionContext(executionContext)
                            .build()) {
                        executionContext.tableAdded(tableName);
                        tableDownloader.download();
                    }
                }
            }
        }

        return new DownloadResponse(
                executionContext.getRowCounts(),
                executionContext.getRowsSkipped(),
                !executionContext.keepRunning()
        );
    }

    private TableExecutionModel toTableExecutionModel(Dependency dependency) {
        return new TableExecutionModel(
                dependency.getConstraintName(),
                dependency.getQueries(),
                dependency.getSubDependencies().stream()
                        .filter(Dependency::isSelected)
                        .map(this::toTableExecutionModel)
                        .collect(Collectors.toCollection(ArrayList::new))
        );
    }

    record DownloadBulkBody(String dataset, List<TableName> tableNames) {}

    public record DownloadTargetBody(String dataset) {}
}
