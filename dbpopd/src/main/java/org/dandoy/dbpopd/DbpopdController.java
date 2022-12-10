package org.dandoy.dbpopd;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.exceptions.HttpStatusException;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.utils.ApplicationException;

import javax.validation.Valid;
import java.util.List;

@Controller
public class DbpopdController {
    private final DbpopdService dbpopdService;

    public DbpopdController(DbpopdService dbpopdService) {
        this.dbpopdService = dbpopdService;
    }

    @Get("populate")
    public DbpopdService.PopulateResult populate(
            List<String> dataset
    ) {
        try {
            return dbpopdService.populate(dataset);
        } catch (ApplicationException e) {
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
    }

    @Post("download")
    public void download(@Valid @Body DownloadRequest downloadRequest) {
        dbpopdService.download(
                downloadRequest.getDataset(),
                new TableName(
                        downloadRequest.getCatalog(),
                        downloadRequest.getSchema(),
                        downloadRequest.getTable()
                ),
                downloadRequest.getWhere()
        );
    }
}