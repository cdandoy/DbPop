package org.dandoy.dbpop.tests;

import org.junit.jupiter.api.Assertions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("UnusedReturnValue")
public class TableAssertion {
    private final Connection connection;
    private final String catalog;
    private final String schema;
    private final String table;

    public TableAssertion(Connection connection, String catalog, String schema, String table) {
        this.connection = connection;
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
    }

    public TableAssertion assertRowCount(int expected) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM %s.%s.%s".formatted(catalog, schema, table))) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                resultSet.next();
                Assertions.assertEquals(expected, resultSet.getInt(1));
                return this;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    public final TableAssertion assertExists(List<String> columns, List<String>... expectedRows) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT %s FROM %s.%s.%s".formatted(String.join(", ", columns), catalog, schema, table))) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                // Fetch the rows
                List<List<String>> rows = new ArrayList<>();
                while (resultSet.next()) {
                    List<String> row = new ArrayList<>();
                    for (int i = 0; i < columns.size(); i++) {
                        String string = resultSet.getString(i + 1);
                        row.add(string);
                    }
                    rows.add(row);
                }

                // Search the rows
                for (List<String> expectedRow : expectedRows) {
                    assertRowExists(rows, expectedRow);
                }

                return this;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertRowExists(List<List<String>> rows, List<String> expectedRow) {
        for (List<String> row : rows) {
            if (row.equals(expectedRow)) {
                return;
            }
        }
        Assertions.fail("Row not found: " + expectedRow);
    }
}
