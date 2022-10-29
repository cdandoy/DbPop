package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.util.List;

@Controller
public class DbpopdController {
    private final DbpopdService dbpopdService;

    public DbpopdController(DbpopdService dbpopdService) {
        this.dbpopdService = dbpopdService;
    }

    @Get("populate")
    public DbpopdService.PopulateResult populate(
            List<String> dataset
    ) {
        return dbpopdService.populate(dataset);
    }
}