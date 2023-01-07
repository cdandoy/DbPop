package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import org.dandoy.dbpop.database.Column;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;

import java.util.List;

@Controller("/database/")
public class DatabaseTableController {
    private final ConfigurationService configurationService;

    public DatabaseTableController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Get("/tables/{catalog}/{schema}/{name}")
    public TableResponse getTable(String catalog, String schema, String name) {
        TableName tableName = new TableName(catalog, schema, name);
        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            Table table = sourceDatabase.getTable(tableName);
            if (table == null) return null;
            return new TableResponse(
                    tableName,
                    table.columns()
                            .stream()
                            .map(Column::getName)
                            .toList()
            );
        }
    }

    public record TableResponse(TableName tableName, List<String> columns) {}
}
