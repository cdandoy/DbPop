package org.dandoy.dbpopd;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.exceptions.HttpStatusException;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.utils.ExceptionUtils;

import javax.validation.Valid;
import java.util.List;

@Controller
@Slf4j
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
        } catch (Exception e) {
            log.error("Failed", e);
            String message = String.join("\n", ExceptionUtils.getErrorMessages(e, ">"));
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, message);
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