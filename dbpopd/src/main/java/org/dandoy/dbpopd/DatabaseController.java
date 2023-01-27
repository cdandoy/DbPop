package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import org.dandoy.dbpop.database.Column;
import org.dandoy.dbpop.database.Dependency;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpopd.database.DatabaseService;
import org.dandoy.dbpopd.database.DependencyCalculator;

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

    @Get("/search")
    public List<SearchTableResponse> search(String query) {
        configurationService.assertSourceConnection();

        return databaseService.getSourceTables()
                .stream().filter(it -> it.getTableName().toQualifiedName().contains(query))
                .map(table -> new SearchTableResponse(
                        table.getTableName().toQualifiedName(),
                        table.getTableName(),
                        table.getColumns().stream().map(Column::getName).toList(),
                        table.getIndexes().stream()
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

        return DependencyCalculator.calculateDependencies(databaseService, dependency);
    }

    public record SearchTableSearchByResponse(String displayName, List<String> columns) {}

    public record SearchTableResponse(String displayName, TableName tableName, List<String> columns, List<SearchTableSearchByResponse> searches) {}
}
