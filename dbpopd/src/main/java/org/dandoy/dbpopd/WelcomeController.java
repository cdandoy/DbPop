package org.dandoy.dbpopd;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.views.View;
import lombok.Getter;
import org.dandoy.dbpop.upload.Populator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller
public class WelcomeController {
    private final DbpopdService dbpopdService;

    public WelcomeController(DbpopdService dbpopdService) {
        this.dbpopdService = dbpopdService;
    }

    @Get(produces = "text/html")
    @View("welcome")
    public HttpResponse<Map<?, ?>> welcome() {
        Populator populator = dbpopdService.getPopulator();
        List<DatasetModel> datasetModels = populator.getDatasetsByName()
                .values()
                .stream()
                .map(it -> new DatasetModel(it.getName()))
                .sorted(Comparator.comparing(it -> it.name))
                .collect(Collectors.toCollection(ArrayList::new));
        if (datasetModels.removeIf(it -> it.name.equals("base"))) datasetModels.add(0, new DatasetModel("base"));
        datasetModels.removeIf(it -> it.name.equals("static"));

        return HttpResponse.ok(
                Map.of(
                        "datasetModels", datasetModels
                )
        );
    }

    @Getter
    public static class DatasetModel {
        private final String name;

        public DatasetModel(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return """
                    <div data-dataset='%s'>
                        <div class='dataset-button'>
                            <button class="btn btn-xs button-load" style="color: limegreen;%s" title="Load">
                                <i class="fa fa-fw fa-play" ></i>
                                <i class="fa fa-fw fa-spinner fa-spin" style='display: none'></i>
                            </button>
                            <span>%s</span>
                        </div>
                        <div class='dataset-result'></div>
                        <div class='dataset-error'></div>
                    </div>"""
                    .formatted(
                            name,
                            name.equals("static") ? "visibility:hidden" : "",
                            name
                    );
        }
    }
}