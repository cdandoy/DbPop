package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
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

    @Get("dependents")
    public List<DependentResponse> getDependents(String catalog, String schema, @QueryValue("table") String tableName) throws SQLException {
        try (Connection connection = configurationService.createConnection()) {
            try (Database database = Database.createDatabase(connection)) {
                Table table = database.getTable(new TableName(catalog, schema, tableName));
                return table.foreignKeys().stream()
                        .map(ForeignKey::getPkTableName)
                        .map(pkTableName -> new DependentResponse(
                                pkTableName.toQualifiedName(),
                                pkTableName,
                                false
                        ))
                        .toList();
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
