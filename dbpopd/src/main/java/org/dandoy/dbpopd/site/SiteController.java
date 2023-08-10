package org.dandoy.dbpopd.site;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import org.dandoy.dbpopd.config.ConfigurationService;

@Controller("/site")
public class SiteController {
    private final ConfigurationService configurationService;
    private final SiteWebSocket siteWebSocket;

    public SiteController(ConfigurationService configurationService, SiteWebSocket siteWebSocket) {
        this.configurationService = configurationService;
        this.siteWebSocket = siteWebSocket;
    }

    @Get
    public SiteResponse site() {
        return new SiteResponse(
                configurationService.hasSourceConnection(),
                configurationService.hasTargetConnection()
        );
    }

    @Get("/status")
    public SiteStatus siteStatus() {
        return siteWebSocket.getSiteStatus();
    }

    public record SiteResponse(boolean hasSource, boolean hasTarget) {}
}
