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

        List<DatasetFileRow> datasetFileRows = new ArrayList<>();
        for (Dataset dataset : datasets) {
            boolean isFirst = true;
            for (DataFile dataFile : dataset.getDataFiles()) {
                File file = dataFile.getFile();
                datasetFileRows.add(
                        new DatasetFileRow(
                                isFirst ? dataset.getName() : "",
                                dataFile.getTableName().toQualifiedName(),
                                file.length(),
                                getCsvRowCount(file)
                        )
                );
                isFirst = false;
            }
        }

        return HttpResponse.ok(
                Map.of(
                        "datasetNames", datasetNames,
                        "datasetFileRows", datasetFileRows
                )
        );
    }

    private static Integer getCsvRowCount(File file) {
        try (CSVParser csvParser = DbPopUtils.createCsvParser(file)) {
            int rows = 0;
            for (CSVRecord ignored : csvParser) {
                rows++;
            }
            return rows;
        } catch (IOException e) {
            log.error("Failed to read " + file);
            return null;
        }
    }

    @Getter
    public static class DatasetFileRow {
        private final String datasetName;
        private final String tableName;
        private final Long fileSize;
        private final Integer rows;

        public DatasetFileRow(String datasetName, String tableName, Long fileSize, Integer rows) {
            this.datasetName = datasetName;
            this.tableName = tableName;
            this.fileSize = fileSize;
            this.rows = rows;
        }
    }
}