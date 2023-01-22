package org.dandoy.dbpopd;

import jakarta.inject.Singleton;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.tests.CsvAssertion;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;

import java.io.File;
import java.util.Optional;
import java.util.stream.Stream;

import static org.dandoy.dbpop.datasets.Datasets.BASE;
import static org.dandoy.dbpop.datasets.Datasets.STATIC;

@Singleton
public class CsvAssertionService {
    private final ConfigurationService configurationService;

    public CsvAssertionService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    private static Optional<File> getFile(Dataset dataset, String tableName) {
        return dataset.getDataFiles().stream()
                .filter(dataFile -> dataFile.getTableName().toQualifiedName().equals(tableName))
                .map(DataFile::getFile)
                .findFirst();
    }

    public CsvAssertion csvAssertion(String tableName) {
        File file = Stream.of(STATIC, BASE)
                .map(it -> Datasets.getDataset(configurationService.getDatasetsDirectory(), it))
                .map(dataset -> getFile(dataset, tableName))
                .filter(Optional::isPresent)
                .map(Optional::orElseThrow)
                .findFirst()
                .orElseThrow();
        return new CsvAssertion(file);
    }

    public CsvAssertion csvAssertion(String datasetName, String tableName) {
        Dataset dataset = Datasets.getDataset(configurationService.getDatasetsDirectory(), datasetName);
        File file = dataset.getDataFiles().stream()
                .filter(dataFile -> dataFile.getTableName().toQualifiedName().equals(tableName))
                .map(DataFile::getFile)
                .findFirst()
                .orElseThrow();
        return new CsvAssertion(file);
    }
}
