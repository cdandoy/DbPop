package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import org.dandoy.dbpop.database.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Controller("/database")
public class DatabaseController {
    private final ConfigurationService configurationService;

    public DatabaseController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Get("search")
    public List<SearchTableResponse> search(String query) throws SQLException {
        try (Connection connection = configurationService.createConnection()) {
            try (Database database = Database.createDatabase(connection)) {
                Set<TableName> tableNames = database.searchTable(query);
                Collection<Table> tables = database.getTables(tableNames);
                return tables.stream()
                        .map(table -> new SearchTableResponse(
                                table.tableName().toQualifiedName(),
                                table.tableName(),
                                table.columns().stream().map(Column::getName).toList(),
                                table.indexes().stream()
                                        .map(index -> new SearchTableSearchByResponse(
                                                "%s (%s)".formatted(index.getName(), String.join(", ", index.getColumns())),
                                                index.getColumns()
                                        ))
                                        .toList()
                        ))
                        .toList();
            }
        }
    }

    @Post("dependents")
    public List<DependentResponse> getDependents(@Body List<TableName> tableNames) throws SQLException {
        try (Connection connection = configurationService.createConnection()) {
            try (Database database = Database.createDatabase(connection)) {
                TableDependencies tableDependencies = new TableDependencies(database);
                tableNames.forEach(tableDependencies::findDependencies);
                return tableDependencies.getDependencies().stream()
                        .map(tableDependency -> {
                            TableName tableName = tableDependency.tableName();
                            return new DependentResponse(
                                    tableName.toQualifiedName(),
                                    tableName,
                                    tableDependency.optional()
                            );
                        }).toList();
            }
        }
    }

    public record SearchTableSearchByResponse(String displayName, List<String> columns) {
    }

    public record SearchTableResponse(String displayName, TableName tableName, List<String> columns, List<SearchTableSearchByResponse> searches) {
    }

    public record DependentResponse(String displayName, TableName tableName, boolean optional) {
    }
}
