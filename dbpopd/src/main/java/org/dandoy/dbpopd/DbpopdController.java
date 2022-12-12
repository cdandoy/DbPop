package org.dandoy.dbpopd;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.exceptions.HttpStatusException;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.download.Downloader;
import org.dandoy.dbpop.download.Where;
import org.dandoy.dbpop.upload.Populator;
import org.dandoy.dbpop.utils.ExceptionUtils;

import javax.validation.Valid;
import java.util.List;
import java.util.Map;

@Controller
@Slf4j
public class DbpopdController {
    private final ConfigurationService configurationService;

    public DbpopdController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Get("populate")
    public PopulateResult populate(
            List<String> dataset
    ) {
        try {
            long t0 = System.currentTimeMillis();
            int rows;
            try (Populator populator = configurationService.createPopulator()) {
                rows = populator.load(dataset);
            }
            long t1 = System.currentTimeMillis();
            return new PopulateResult(rows, t1 - t0);
        } catch (Exception e) {
            log.error("Failed", e);
            String message = String.join("\n", ExceptionUtils.getErrorMessages(e, ">"));
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, message);
        }
    }

    @Post("download")
    public void download(@Valid @Body DownloadRequest downloadRequest) {
        String dataset = downloadRequest.getDataset();
        TableName tableName = new TableName(
                downloadRequest.getCatalog(),
                downloadRequest.getSchema(),
                downloadRequest.getTable()
        );
        Map<String, Object> whereMap = downloadRequest.getWhere();
        try (Downloader downloader = configurationService.createDownloadBuilder()
                .setDataset(dataset)
                .build()) {
            List<Where> wheres = whereMap.entrySet().stream()
                    .map(entry -> new Where(entry.getKey(), entry.getValue()))
                    .toList();
            downloader.download(tableName, wheres);
        }
    }

    record PopulateResult(int rows, long millis) {
    }
}