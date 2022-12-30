package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.util.Map;

@Controller
public class SiteController {

    @Get("/site")
    public Map<String, Object> site() {
        return Map.of(
                "mode", "download"
        );
    }
}
