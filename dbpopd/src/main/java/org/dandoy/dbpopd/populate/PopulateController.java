package org.dandoy.dbpopd.populate;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.problem.HttpStatusType;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.utils.MultiCauseException;
import org.zalando.problem.Problem;

import java.util.List;

@Controller
@Slf4j
@Tag(name = "populate")
public class PopulateController {
    private final PopulateService populateService;

    public PopulateController(PopulateService populateService) {
        this.populateService = populateService;
    }

    @Get("/populate")
    public PopulateResult populate(List<String> dataset) {
        try {
            return populateService.populate(dataset);
        } catch (Exception e) {
            log.error("Failed", e);
            throw Problem.builder()
                    .withStatus(new HttpStatusType(HttpStatus.BAD_REQUEST))
                    .withDetail(String.join("\n", MultiCauseException.getCauses(e)))
                    .build();
        }
    }
}