package org.dandoy.dbpop.download;

import lombok.Getter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.utils.DbPopUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;

@Getter
public class OutputFile {
    private final File file;
    private List<String> headers;
    private final boolean newFile;

    private OutputFile(File file, List<String> headers, boolean newFile) {
        this.file = file;
        this.headers = headers;
        this.newFile = newFile;
    }

    public static OutputFile createOutputFile(File datasetsDirectory, String dataset, TableName tableName) {
        File file = DbPopUtils.getOutputFile(datasetsDirectory, dataset, tableName);
        if (file.exists()) {
            List<String> headers = readHeaders(file);
            return new OutputFile(file, headers, false);
        } else {
            return new OutputFile(file, null, true);
        }
    }

    private static List<String> readHeaders(File file) {
        try (CSVParser csvParser = DbPopUtils.createCsvParser(file)) {
            return csvParser.getHeaderNames();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + file, e);
        }
    }

    public void setColumns(List<SelectedColumn> selectedColumns) {
        headers = selectedColumns.stream().map(SelectedColumn::asHeaderName).toList();
    }

    public CSVPrinter createCsvPrinter() throws IOException {
        boolean newFile = !file.exists();
        BufferedWriter bufferedWriter = Files.newBufferedWriter(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        CSVFormat.Builder csvFormatBuilder = CSVFormat.DEFAULT.builder()
                .setNullString("");
        if (newFile) {
            csvFormatBuilder.setHeader(headers.toArray(String[]::new));
        }
        return new CSVPrinter(bufferedWriter, csvFormatBuilder.build());
    }
}
