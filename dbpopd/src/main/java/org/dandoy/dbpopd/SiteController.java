package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.util.Map;

@Controller
public class SiteController {
    private final SqlSetupService sqlSetupService;
    private final ConfigurationService configurationService;

    public SiteController(ConfigurationService configurationService,
                          SqlSetupService sqlSetupService) {
        this.configurationService = configurationService;
        this.sqlSetupService = sqlSetupService;
    }

    @Get("/site")
    public Map<String, Object> site() {
        return Map.of(
                "mode", configurationService.getMode()
        );
    }

    /**
     * Gets the status of the SqlSetupService, the service that runs setup.sql in populate mode
     */
    @Get("/site/populate/setup")
    public SqlSetupStatus setupStatus() {
        return new SqlSetupStatus(
                sqlSetupService.isLoading(),
                sqlSetupService.isLoaded(),
                sqlSetupService.getErrorMessage()
        );
    }

    public record SqlSetupStatus(boolean loading, boolean loaded, String errorMessage) {
    }
}
