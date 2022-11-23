package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import org.dandoy.dbpop.database.TableName;

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
        return dbpopdService.populate(dataset);
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