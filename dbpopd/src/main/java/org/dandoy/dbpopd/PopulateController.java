package org.dandoy.dbpopd;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.exceptions.HttpStatusException;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.utils.ExceptionUtils;

import java.util.List;

@Controller
@Slf4j
public class PopulateController {
    private final PopulatorHolder populatorHolder;
    private final SqlSetupService sqlSetupService;

    public PopulateController(PopulatorHolder populatorHolder,
                              SqlSetupService sqlSetupService) {
        this.populatorHolder = populatorHolder;
        this.sqlSetupService = sqlSetupService;
    }

    @Get("/populate")
    public PopulateResult populate(List<String> dataset) {
        try {
            long t0 = System.currentTimeMillis();
            int rows = populatorHolder.getPopulator().load(dataset);
            long t1 = System.currentTimeMillis();
            return new PopulateResult(rows, t1 - t0);
        } catch (Exception e) {
            log.error("Failed", e);
            String message = String.join("\n", ExceptionUtils.getErrorMessages(e, ">"));
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, message);
        }
    }

    /**
     * Used for tests only
     */
    public void resetPopulatorHolder() {
        populatorHolder.reset();
    }

    /**
     * Gets the status of the SqlSetupService, the service that runs setup.sql in populate mode
     */
    @Get("/site/populate/setup")
    public SqlSetupStatus setupStatus() {
        return new SqlSetupStatus(
                sqlSetupService.isConnected(),
                sqlSetupService.isLoaded(),
                sqlSetupService.getErrorMessage()
        );
    }

    public record SqlSetupStatus(boolean connected, boolean loaded, String errorMessage) {}

    record PopulateResult(int rows, long millis) {}
}