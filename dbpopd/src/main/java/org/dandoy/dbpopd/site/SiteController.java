package org.dandoy.dbpopd.site;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.DatabaseCacheService;

@Controller("/site")
public class SiteController {
    private final ConfigurationService configurationService;
    private final DatabaseCacheService databaseCacheService;

    public SiteController(ConfigurationService configurationService, DatabaseCacheService databaseCacheService) {
        this.configurationService = configurationService;
        this.databaseCacheService = databaseCacheService;
    }

    @Get
    public SiteResponse site() {
        // Clear the database cache when the main page is reloaded
        databaseCacheService.clearSourceDatabaseCache();
        databaseCacheService.clearTargetDatabaseCache();

        return new SiteResponse(
                configurationService.hasSourceConnection(),
                configurationService.hasTargetConnection()
        );
    }

    public record SiteResponse(boolean hasSource, boolean hasTarget) {}
}
