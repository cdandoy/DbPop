package org.dandoy.dbpopd;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.views.View;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpop.utils.DbPopUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller
@Slf4j
public class WelcomeController {
    private final ConfigurationService configurationService;

    public WelcomeController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Get(produces = "text/html")
    @View("welcome")
    public HttpResponse<Map<?, ?>> welcome() {
        // Get the list of datasets
        List<Dataset> datasets = Datasets.getDatasets(configurationService.getDatasetsDirectory())
                .stream()
                .filter(it -> !"static".equals(it.getName()))
                .sorted(Comparator.comparing(Dataset::getName))
                .collect(Collectors.toCollection(ArrayList::new));

        // move "base" to the top
        datasets.stream()
                .filter(it -> it.getName().equals("base"))
                .findFirst()
                .ifPresent(it -> {
                    datasets.remove(it);
                    datasets.add(0, it);
                });

        List<String> datasetNames = datasets.stream()
                .map(Dataset::getName)
                .toList();

        List<DatasetStatus> datasetStatuses = datasets.stream()
                .map(DatasetStatus::new)
                .toList();

        return HttpResponse.ok(
                Map.of(
                        "datasetNames", datasetNames,
                        "datasetStatuses", datasetStatuses
                )
        );
    }

    @Getter
    public static class DatasetStatus {
        private final String name;
        private final List<DatasetFileStatus> datasetFileStatuses;

        public DatasetStatus(Dataset dataset) {
            name = dataset.getName();
            datasetFileStatuses = dataset.getDataFiles().stream()
                    .map(DatasetFileStatus::new)
                    .toList();
        }
    }

    @Getter
    public static class DatasetFileStatus {
        private final long size;
        private final int rows;
        private final String tableName;

        public DatasetFileStatus(DataFile dataFile) {
            File file = dataFile.getFile();
            size = file.length();
            int rows = 0;
            try (CSVParser csvParser = DbPopUtils.createCsvParser(file)) {
                for (CSVRecord ignored : csvParser) {
                    rows++;
                }
            } catch (IOException e) {
                log.error("Failed to read " + file);
            }
            this.rows = rows;
            tableName = dataFile.getTableName().toQualifiedName();
        }
    }
}