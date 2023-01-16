package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpopd.database.DatabaseService;

import java.util.Comparator;
import java.util.List;

@Controller("/database")
public class DatabaseController {
    private final ConfigurationService configurationService;
    private final DatabaseService databaseService;

    public DatabaseController(ConfigurationService configurationService,
                              DatabaseService databaseService) {
        this.configurationService = configurationService;
        this.databaseService = databaseService;
    }

    @Get("" +
         "search")
    public List<SearchTableResponse> search(String query) {
        configurationService.assertSourceConnection();

        return databaseService.getSourceTables()
                .stream().filter(it -> it.tableName().toQualifiedName().contains(query))
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
                .sorted(Comparator.comparing(searchTableResponse -> searchTableResponse.tableName().toQualifiedName().length()))
                .toList();
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
