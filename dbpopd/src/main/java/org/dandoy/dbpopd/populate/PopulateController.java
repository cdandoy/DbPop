package org.dandoy.dbpopd.populate;

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
    private final PopulateService populateService;

    public PopulateController(PopulateService populateService) {
        this.populateService = populateService;
    }

    @Get("/populate")
    public PopulateService.PopulateResult populate(List<String> dataset) {
        try {
            return populateService.populate(dataset);
        } catch (Exception e) {
            log.error("Failed", e);
            String message = String.join("\n", ExceptionUtils.getErrorMessages(e, ">"));
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, message);
        }
    }
}