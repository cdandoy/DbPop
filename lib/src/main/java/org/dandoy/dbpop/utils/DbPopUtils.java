package org.dandoy.dbpop.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DbPopUtils {

    /**
     * Returns the position (starting at 0) of the column name in metaData or -1 if it does not exist.
     */
    public static int getPositionByColumnName(ResultSetMetaData metaData, String columnName) {
        try {
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                if (columnName.equals(metaData.getColumnName(i + 1))) {
                    return i;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return -1;
    }

    public static CSVParser createCsvParser(File file) throws IOException {
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setNullString("")
                .build();

        return csvFormat.parse(Files.newBufferedReader(file.toPath(), UTF_8));
    }
}
