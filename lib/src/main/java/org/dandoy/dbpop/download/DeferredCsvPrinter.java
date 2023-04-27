package org.dandoy.dbpop.download;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.dandoy.dbpop.FeatureFlags;

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

    private CSVPrinter create(boolean writeHeaders) {
        try {
            boolean newFile = !file.exists();
            BufferedWriter bufferedWriter = Files.newBufferedWriter(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            CSVFormat.Builder csvFormatBuilder = CSVFormat.DEFAULT.builder()
                    .setNullString("");
            if (newFile && writeHeaders) {
                csvFormatBuilder.setHeader(headers.toArray(String[]::new));
            }
            return new CSVPrinter(bufferedWriter, csvFormatBuilder.build());
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
        } else {
            if (FeatureFlags.createCsvFilesForEmptyTables) {
                if (!file.exists()) {
                    try {
                        create(FeatureFlags.includeCsvHeadersForEmptyTables)
                                .close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private CSVPrinter getCsvPrinter() {
        if (csvPrinter == null) {
            csvPrinter = create(true);
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
