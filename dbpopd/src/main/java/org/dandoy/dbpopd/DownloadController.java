package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Dependency;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.download.ExecutionMode;
import org.dandoy.dbpop.download.ExecutionPlan;
import org.dandoy.dbpop.download.TableExecutionModel;

import java.util.*;
import java.util.stream.Collectors;

@Controller
public class DownloadController {
    private final ConfigurationService configurationService;

    public DownloadController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Post("/download")
    public DownloadResponse download(@Body DownloadRequest downloadRequest) {
        configurationService.assertSourceConnection();

        ExecutionMode executionMode = downloadRequest.isDryRun() ? ExecutionMode.COUNT : ExecutionMode.SAVE;
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
            Map<TableName, Integer> executionResult = ExecutionPlan.execute(
                    sourceDatabase,
                    configurationService.getDatasetsDirectory(),
                    downloadRequest.getDataset(),
                    downloadRequest.getDependency().getTableName(),
                    tableExecutionModel,
                    filteredColumns,
                    pks,
                    executionMode,
                    1000
            );
            return new DownloadResponse(executionResult);
        }
    }

    private TableExecutionModel toTableExecutionModel(Dependency dependency) {
        return new TableExecutionModel(
                dependency.getConstraintName(),
                dependency.getSubDependencies().stream()
                        .filter(Dependency::isSelected)
                        .map(this::toTableExecutionModel)
                        .collect(Collectors.toCollection(ArrayList::new))
        );
    }
}
