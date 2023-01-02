package org.dandoy.dbpop.download;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * A CSV printer that only creates the file if needed
 */
public class DeferredCsvPrinter implements AutoCloseable {
    private final File file;
    private final List<String> headers;
    private CSVPrinter csvPrinter;

    public DeferredCsvPrinter(File file, List<String> headers) {
        this.file = file;
        this.headers = headers;
    }

    public void create() {
        try {
            boolean newFile = !file.exists();
            BufferedWriter bufferedWriter = Files.newBufferedWriter(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            CSVFormat.Builder csvFormatBuilder = CSVFormat.DEFAULT.builder()
                    .setNullString("");
            if (newFile) {
                csvFormatBuilder.setHeader(headers.toArray(String[]::new));
            }
            csvPrinter = new CSVPrinter(bufferedWriter, csvFormatBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create a DeferredCsvPrinter for " + file, e);
        }
    }

    @Override
    public void close() {
        if (csvPrinter != null) {
            try {
                csvPrinter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private CSVPrinter getCsvPrinter() {
        if (csvPrinter == null) {
            create();
        }
        return csvPrinter;
    }

    public void print(Object o) throws IOException {
        getCsvPrinter().print(o);
    }

    public void println() throws IOException {
        getCsvPrinter().println();
    }
}
