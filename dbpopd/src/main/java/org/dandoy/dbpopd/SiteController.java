package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.util.Map;

@Controller("/site")
public class SiteController {
    private final String mode;
    private final SqlSetupService sqlSetupService;

    public SiteController(ConfigurationService configurationService,
                          SqlSetupService sqlSetupService) {
        this.mode = configurationService.getMode();
        this.sqlSetupService = sqlSetupService;
    }

    @Get
    public Map<String, Object> site() {
        return Map.of(
                "mode", mode
                );
    }

    /**
     * Gets the status of the SqlSetupService, the service that runs setup.sql in populate mode
     */
    @Get("/populate/setup")
    public WelcomeController.SqlSetupStatus setupStatus() {
        // TODO: We still share this with the jQuery version of the app. Move it over when we switch
        return new WelcomeController.SqlSetupStatus(
                sqlSetupService.isLoading(),
                sqlSetupService.isLoaded(),
                sqlSetupService.getErrorMessage()
        );
    }
}
