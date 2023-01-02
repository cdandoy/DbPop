package org.dandoy.dbpop.download;

import lombok.Getter;
import org.apache.commons.csv.CSVParser;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.utils.DbPopUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Getter
public class OutputFile {
    private final File file;
    private List<String> headers;
    private final boolean forceCreate;

    /**
     * @param file       The file to write to
     * @param headers    The CSV headers
     * @param forceEmpty Force the creation of the file, even if it is empty
     */
    private OutputFile(File file, List<String> headers, boolean forceEmpty) {
        this.file = file;
        this.headers = headers;
        this.forceCreate = forceEmpty;
    }

    public boolean isNewFile() {
        return !file.exists();
    }

    public static OutputFile createOutputFile(File datasetsDirectory, String dataset, TableName tableName, boolean forceEmpty) {
        File file = DbPopUtils.getOutputFile(datasetsDirectory, dataset, tableName);
        if (file.exists()) {
            List<String> headers = readHeaders(file);
            return new OutputFile(file, headers, forceEmpty);
        } else {
            return new OutputFile(file, null, forceEmpty);
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

    public DeferredCsvPrinter createCsvPrinter() {
        return new DeferredCsvPrinter(file, headers);
    }
}
