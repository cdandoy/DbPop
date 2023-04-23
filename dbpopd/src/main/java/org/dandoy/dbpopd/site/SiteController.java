package org.dandoy.dbpopd.site;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import org.dandoy.dbpopd.ConfigurationService;

@Controller("/site")
public class SiteController {
    private final ConfigurationService configurationService;

    public SiteController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Get
    public SiteResponse site() {
        // Clear the database cache when the main page is reloaded
        configurationService.clearSourceDatabaseCache();
        configurationService.clearTargetDatabaseCache();

        return new SiteResponse(
                configurationService.hasSourceConnection(),
                configurationService.hasTargetConnection()
        );
    }

    public record SiteResponse(boolean hasSource, boolean hasTarget) {}
}
