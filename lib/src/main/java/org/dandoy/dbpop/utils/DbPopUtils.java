package org.dandoy.dbpop.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.dandoy.dbpop.database.TableName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@Slf4j
public class DbPopUtils {

    public static CSVParser createCsvParser(File file) throws IOException {
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setNullString("")
                .build();

        return csvFormat.parse(Files.newBufferedReader(file.toPath()));
    }

    public static File getOutputFile(File datasetsDirectory, String dataset, TableName tableName) {
        File dir = new File(datasetsDirectory, dataset);
        if (tableName.getCatalog() != null) dir = new File(dir, tableName.getCatalog());
        if (tableName.getSchema() != null) dir = new File(dir, tableName.getSchema());

        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new RuntimeException("Cannot create the directory " + dir);
        }
        return new File(dir, tableName.getTable() + ".csv");
    }

    public static Integer getCsvRowCount(File file) {
        try (CSVParser csvParser = createCsvParser(file)) {
            int rows = 0;
            for (CSVRecord ignored : csvParser) {
                rows++;
            }
            return rows;
        } catch (Exception e) {
            log.error("Failed to read " + file);
            return null;
        }
    }
}
