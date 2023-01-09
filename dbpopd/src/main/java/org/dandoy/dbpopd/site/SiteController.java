package org.dandoy.dbpopd.site;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.setup.SetupService;

@Controller("/site")
public class SiteController {
    private final ConfigurationService configurationService;
    private final SetupService setupService;

    public SiteController(ConfigurationService configurationService,SetupService setupService) {
        this.configurationService = configurationService;
        this.setupService = setupService;
    }

    @Get
    public SiteResponse site() {
        return new SiteResponse(
                configurationService.hasSourceConnection(),
                configurationService.hasTargetConnection()
        );
    }

    @Get("/status")
    public SetupService.SetupState getSetupStatus() {
        return setupService.getSetupState();
    }

    public record SiteResponse(boolean hasSource, boolean hasTarget) {}
}
