package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.util.Map;

@Controller
public class SiteController {
    private final ConfigurationService configurationService;

    public SiteController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Get("/site")
    public Map<String, Object> site() {
        return Map.of(
                "mode", configurationService.getMode()
        );
    }
}
