package org.dandoy.dbpopd;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.exceptions.HttpStatusException;

@Controller
@Context
@Requires(property = "dbpopd.mode", value = "download")
public class DownloadController {
    public DownloadController() {
    }

    @Requires(property = "dbpopd.mode", value = "download")
    @Get("download")
    public void download(DownloadRequest downloadRequest) {
        throw new HttpStatusException(HttpStatus.BAD_REQUEST, "Not implemented");
    }
}
