package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;

@Controller("/database/")
public class DatabaseTableController {
    private final ConfigurationService configurationService;

    public DatabaseTableController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Get("/tables/{catalog}/{schema}/{name}")
    public Table getTable(String catalog, String schema, String name) {
        TableName tableName = new TableName(catalog, schema, name);
        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            return sourceDatabase.getTable(tableName);
        }
    }
}
