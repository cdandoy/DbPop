package org.dandoy.dbpopd.content;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import org.dandoy.dbpop.database.ForeignKey;
import org.dandoy.dbpop.database.RowCount;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.database.DatabaseService;
import org.dandoy.dbpopd.datasets.FileCacheService;

import java.util.Collection;
import java.util.List;

@Controller
public class ContentController {
    private final ConfigurationService configurationService;
    private final DatabaseService databaseService;
    private final FileCacheService fileCacheService;

    public ContentController(ConfigurationService configurationService, DatabaseService databaseService, FileCacheService fileCacheService) {
        this.configurationService = configurationService;
        this.databaseService = databaseService;
        this.fileCacheService = fileCacheService;
    }

    @Get("/content/")
    public List<TableInfo> getContent() {
        List<Dataset> datasets = Datasets.getDatasets(configurationService.getDatasetsDirectory());

        Collection<Table> sourceTables = databaseService.getSourceTables();
        return sourceTables.stream()
                .map(table -> {
                    TableName tableName = table.getTableName();
                    return new TableInfo(
                            tableName,
                            databaseService.getSourceRowCount(tableName),
                            countCsvRows(datasets, Datasets.STATIC, tableName),
                            countCsvRows(datasets, Datasets.BASE, tableName),
                            getDependencies(table)
                    );
                })
                .sorted()
                .toList();
    }

    private static List<TableName> getDependencies(Table table) {
        return table.getForeignKeys()
                .stream()
                .map(ForeignKey::getPkTableName)
                .filter(tableName -> !table.getTableName().equals(tableName))
                .toList();
    }

    private RowCount countCsvRows(List<Dataset> datasets, String datasetName, TableName tableName) {
        int rows = 0;
        for (Dataset dataset : datasets) {
            if (datasetName.equals(dataset.getName())) {
                for (DataFile dataFile : dataset.getDataFiles()) {
                    if (tableName.equals(dataFile.getTableName())) {
                        Integer csvRowCount = fileCacheService.getFileInfo(dataFile.getFile()).rowCount();
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
