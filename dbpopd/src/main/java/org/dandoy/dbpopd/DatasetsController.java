package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
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
import java.util.stream.Collectors;

@Controller("/datasets")
@Slf4j
public class DatasetsController {
    private final ConfigurationService configurationService;

    public DatasetsController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Get
    public List<String> getDatasets() {
        return Datasets.getDatasets(configurationService.getDatasetsDirectory())
                .stream().map(Dataset::getName)
                .toList();
    }

    @Get("/files")
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
        return datasetFileRows;
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

    public record DatasetFileRow(String datasetName, String tableName, Long fileSize, Integer rows) {
    }
}
