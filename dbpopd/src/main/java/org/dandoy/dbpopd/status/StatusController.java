package org.dandoy.dbpopd.status;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.util.Map;

@Controller
public class StatusController {
    private final StatusService statusService;

    public StatusController(StatusService statusService) {
        this.statusService = statusService;
    }

    @Get("/status")
    public Map<String, Object> getStatus() {
        return Map.of(
                "statuses", statusService.getStatuses(),
                "complete", statusService.isComplete()
        );
    }
}
