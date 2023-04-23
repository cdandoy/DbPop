package org.dandoy.dbpop.datasets;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.upload.DataFile;
import org.dandoy.dbpop.upload.Dataset;
import org.dandoy.dbpop.utils.StopWatch;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class Datasets {
    public static final String STATIC = "static";
    public static final String BASE = "base";
    public static final Comparator<String> DATASET_NAME_COMPARATOR = (s1, s2) -> {
        if (STATIC.equals(s1)) return -2;
        if (STATIC.equals(s2)) return 2;
        if (BASE.equals(s1)) return -1;
        if (BASE.equals(s2)) return 1;
        return s1.compareTo(s2);
    };

    public static Dataset getDataset(File directory, String name) {
        return getDataset(new File(directory, name));
    }

    public static List<Dataset> getDatasets(File directory) {
        List<Dataset> datasetList = StopWatch.record("DatasetUtils.getDatasets", () -> {
            List<Dataset> datasets = new ArrayList<>();
            File[] datasetFiles = directory.listFiles();
            if (datasetFiles == null) {
                if (directory.exists()) throw new RuntimeException("Invalid path " + directory);
                else return Collections.emptyList();
            }
            for (File datasetFile : datasetFiles) {
                Dataset dataset = getDataset(datasetFile);
                if (dataset != null) {
                    datasets.add(dataset);
                }
            }
            return datasets;
        });

        if (getByName(datasetList, BASE).isEmpty()) datasetList.add(0, new Dataset(BASE, Collections.emptyList()));
        if (getByName(datasetList, STATIC).isEmpty()) datasetList.add(0, new Dataset(STATIC, Collections.emptyList()));

        return datasetList;
    }

    private static Optional<Dataset> getByName(Collection<Dataset> datasets, String name) {
        for (Dataset dataset : datasets) {
            if (name.equals(dataset.getName())) return Optional.of(dataset);
        }
        return Optional.empty();
    }

    private static Dataset getDataset(File datasetFile) {
        File[] catalogFiles = datasetFile.listFiles();
        if (catalogFiles == null) return null;

        Collection<DataFile> dataFiles = new ArrayList<>();
        for (File catalogFile : catalogFiles) {
            String catalog = catalogFile.getName();
            File[] schemaFiles = catalogFile.listFiles();
            if (schemaFiles != null) {
                for (File schemaFile : schemaFiles) {
                    String schema = schemaFile.getName();
                    File[] tableFiles = schemaFile.listFiles();
                    if (tableFiles != null) {
                        for (File tableFile : tableFiles) {
                            String tableFileName = tableFile.getName();
                            if (tableFileName.endsWith(".csv")) {
                                String table = tableFileName.substring(0, tableFileName.length() - 4);
                                dataFiles.add(
                                        new DataFile(
                                                tableFile,
                                                new TableName(catalog, schema, table)
                                        )
                                );
                            }
                        }
                    } else {
                        log.warn("Unexpected file " + schemaFile);
                    }
                }
            } else {
                log.warn("Unexpected file " + catalogFile);
            }
        }

        return new Dataset(
                datasetFile.getName(),
                dataFiles
        );
    }

    /**
     * Check that we have all the tables that are in the dataset
     *
     * @param allDatasets       The datasets
     * @param datasetTableNames The table names found in the data sets
     * @param databaseTables    the tables found in the database that are in the data sets
     */
    public static void validateAllTablesExist(List<Dataset> allDatasets, Set<TableName> datasetTableNames, Collection<Table> databaseTables) {
        Set<TableName> databaseTableNames = databaseTables.stream().map(Table::getTableName).collect(Collectors.toSet());
        List<TableName> missingTables = datasetTableNames.stream()
                .filter(tableName -> !databaseTableNames.contains(tableName))
                .toList();
        if (!missingTables.isEmpty()) {
            DataFile badDataFile = allDatasets.stream()
                    .flatMap(dataset -> dataset.getDataFiles().stream())
                    .filter(dataFile -> missingTables.contains(dataFile.getTableName()))
                    .findFirst()
                    .orElseThrow(RuntimeException::new);
            throw new MissingTablesException(
                    badDataFile,
                    String.format(
                            "Table %s does not exist for this data file %s",
                            badDataFile.getTableName().toQualifiedName(),
                            badDataFile.getFile()
                    ));
        }
    }
}
