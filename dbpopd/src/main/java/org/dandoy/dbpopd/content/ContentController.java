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
import org.dandoy.dbpopd.DatasetsController;
import org.dandoy.dbpopd.database.DatabaseService;

import java.util.Collection;
import java.util.List;
import java.util.Set;

@Controller
public class ContentController {
    private final ConfigurationService configurationService;
    private final DatabaseService databaseService;

    public ContentController(ConfigurationService configurationService, DatabaseService databaseService) {
        this.configurationService = configurationService;
        this.databaseService = databaseService;
    }

    @Get("/content/")
    public List<TableInfo> getContent() {
        List<Dataset> datasets = Datasets.getDatasets(configurationService.getDatasetsDirectory());

        Collection<Table> sourceTables = databaseService.getSourceTables();
        Set<TableName> targetTableNames = databaseService.getTargetTableNames();
        return sourceTables.stream()
                .map(table -> {
                    TableName tableName = table.tableName();
                    return new TableInfo(
                            tableName,
                            databaseService.getSourceRowCount(tableName),
                            targetTableNames.contains(tableName) ? databaseService.getTargetRowCount(tableName) : new RowCount(0, false),
                            countCsvRows(datasets, Datasets.STATIC, tableName),
                            countCsvRows(datasets, Datasets.BASE, tableName),
                            getDependencies(table)
                    );
                })
                .sorted()
                .toList();
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
