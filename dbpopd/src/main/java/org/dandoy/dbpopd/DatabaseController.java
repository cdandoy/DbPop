package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import org.dandoy.dbpop.database.*;

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
    public List<SearchTableResponse> search(String query) {
        configurationService.assertSourceConnection();

        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            Set<TableName> tableNames = sourceDatabase.searchTable(query);
            Collection<Table> tables = sourceDatabase.getTables(tableNames);
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

    @Post("dependencies")
    public Dependency getDependents(@Body Dependency dependency) {
        configurationService.assertSourceConnection();

        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            return DependencyCalculator.calculateDependencies(sourceDatabase, dependency);
        }
    }

    public record SearchTableSearchByResponse(String displayName, List<String> columns) {}

    public record SearchTableResponse(String displayName, TableName tableName, List<String> columns, List<SearchTableSearchByResponse> searches) {}
}
