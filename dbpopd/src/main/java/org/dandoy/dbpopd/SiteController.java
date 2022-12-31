package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

@Controller
public class SiteController {
    private final ConfigurationService configurationService;

    public SiteController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Get("/site")
    public SiteResponse site() {
        return new SiteResponse(
                configurationService.hasSourceConnection(),
                configurationService.hasTargetConnection()
        );
    }

    public record SiteResponse(boolean hasSource, boolean hasTarget) {}
}
