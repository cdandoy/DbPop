package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpopd.database.DatabaseService;

@Controller("/database/")
@Tag(name = "database")
public class DatabaseTableController {
    private final DatabaseService databaseService;

    public DatabaseTableController(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Get("/tables/{catalog}/{schema}/{name}")
    public Table getTable(String catalog, String schema, String name) {
        TableName tableName = new TableName(catalog, schema, name);
        return databaseService.getSourceTable(tableName);
    }
}
