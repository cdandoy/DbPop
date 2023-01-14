package org.dandoy.dbpopd.content;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.DatasetsController;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Controller
public class ContentController {
    private final ConfigurationService configurationService;

    public ContentController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Get("/content/")
    public List<TableInfo> getContent() {
        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            try (Database targetDatabase = configurationService.createTargetDatabase()) {
                List<Dataset> datasets = Datasets.getDatasets(configurationService.getDatasetsDirectory());

                Collection<Table> sourceTables = sourceDatabase.getTables();
                Set<TableName> targetTableNames = targetDatabase.getTables().stream().map(Table::tableName).collect(Collectors.toSet());
                return sourceTables.stream()
                        .map(table -> {
                            TableName tableName = table.tableName();
                            return new TableInfo(
                                    tableName,
                                    sourceDatabase.getRowCount(tableName),
                                    targetTableNames.contains(tableName) ? targetDatabase.getRowCount(tableName) : new RowCount(0, false),
                                    countCsvRows(datasets, Datasets.STATIC, tableName),
                                    countCsvRows(datasets, Datasets.BASE, tableName),
                                    getDependencies(table)
                            );
                        })
                        .sorted()
                        .toList();
            }
        }
    }

    private static List<TableName> getDependencies(Table table) {
        return table.foreignKeys()
                .stream()
                .map(ForeignKey::getPkTableName)
                .filter(tableName -> !table.tableName().equals(tableName))
                .toList();
    }

    private static RowCount countCsvRows(List<Dataset> datasets, String datasetName, TableName tableName) {
        int rows = 0;
        for (Dataset dataset : datasets) {
            if (datasetName.equals(dataset.getName())) {
                for (DataFile dataFile : dataset.getDataFiles()) {
                    if (tableName.equals(dataFile.getTableName())) {
                        Integer csvRowCount = DatasetsController.getCsvRowCount(dataFile.getFile());
                        if (csvRowCount != null) {
                            rows += csvRowCount;
                        }
                    }
                }
            }
        }
        return new RowCount(rows, false);
    }
}
