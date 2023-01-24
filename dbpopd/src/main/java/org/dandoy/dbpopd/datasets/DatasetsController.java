package org.dandoy.dbpopd.datasets;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Controller("/datasets")
@Slf4j
public class DatasetsController {
    private final DatasetsService datasetsService;

    public DatasetsController(DatasetsService datasetsService) {
        this.datasetsService = datasetsService;
    }

    @Get
    public List<String> getDatasets() {
        return datasetsService.getDatasets();
    }

    @Get("/content")
    public DatasetContentResponse getContent() {
        return datasetsService.getContent();
    }
}
