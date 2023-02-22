package org.dandoy.dbpop.tests;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;

@SuppressWarnings("UnusedReturnValue")
public class CsvAssertion {
    private final Map<String, Integer> columnNamesToPosition;
    private final List<List<String>> rows;

    public CsvAssertion(File file) {
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setNullString("")
                .build();
        columnNamesToPosition = new HashMap<>();
        rows = new ArrayList<>();
        try (CSVParser csvParser = csvFormat.parse(Files.newBufferedReader(file.getCanonicalFile().toPath()))) {
            List<String> headerNames = csvParser.getHeaderNames();
            for (int i = 0; i < headerNames.size(); i++) {
                String headerName = headerNames.get(i);
                columnNamesToPosition.put(headerName, i);
            }
            for (CSVRecord csvRecord : csvParser) {
                List<String> row = new ArrayList<>();
                for (String s : csvRecord) {
                    row.add(s);
                }
                rows.add(row);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean matches(List<String> columns, List<String> row1, List<String> row2) {
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            Integer pos = columnNamesToPosition.get(column);
            if (!Objects.equals(row1.get(pos), row2.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean findRow(List<String> columns, List<String> search) {
        for (List<String> row : rows) {
            if (matches(columns, row, search)) {
                return true;
            }
        }
        return false;
    }

    private void assertColumnsExist(List<String> columns) {
        for (String column : columns) {
            if (!columnNamesToPosition.containsKey(column)) {
                throw new RuntimeException("Column not found: " + column);
            }
        }
    }

    @SafeVarargs
    public final CsvAssertion assertExists(List<String> columns, List<String>... expected) {
        assertColumnsExist(columns);
        for (List<String> expectedRow : expected) {
            Assertions.assertTrue(findRow(columns, expectedRow), "Row not found: " + expectedRow);
        }
        return this;
    }

    public CsvAssertion assertUnique(String... columns) {
        assertColumnsExist(List.of(columns));
        List<List<String>> all = rows.stream()
                .map(row -> extract(row, columns))
                .toList();
        Set<List<String>> unique = new HashSet<>(all);
        Assertions.assertEquals(all.size(), unique.size());
        return this;
    }

    private List<String> extract(List<String> row, String... columns) {
        return Stream.of(columns)
                .map(column -> row.get(columnNamesToPosition.get(column)))
                .toList();
    }

    public CsvAssertion assertRowCount(int expected) {
        Assertions.assertEquals(expected, rows.size());
        return this;
    }
}
