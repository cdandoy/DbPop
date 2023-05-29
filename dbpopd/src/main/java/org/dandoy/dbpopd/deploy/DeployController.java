package org.dandoy.dbpopd.deploy;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.http.server.types.files.SystemFile;
import io.micronaut.problem.HttpStatusType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dandoy.dbpopd.ConfigurationService;
import org.zalando.problem.Problem;

import java.io.File;

@Controller("/deploy/")
@Slf4j
public class DeployController {
    private final ConfigurationService configurationService;
    private final DeployService deployService;

    public DeployController(ConfigurationService configurationService, DeployService deployService) {
        this.configurationService = configurationService;
        this.deployService = deployService;
    }

    public record GetDeployResponse(boolean hasSnapshot, boolean hasChanges, Long timestamp, String snapshotFilename, DeltaType deltaType) {
        public GetDeployResponse(String snapshotFilename) {
            this(false, false, null, snapshotFilename, null);
        }
    }

    @Get
    public GetDeployResponse getDeploy() {
        String snapshotFilename = configurationService.getSnapshotFile().toString();
        if (snapshotFilename.startsWith("/var/opt/dbpop/")) {
            snapshotFilename = snapshotFilename.substring("/var/opt/dbpop/".length());
        }

        SnapshotInfo snapshotInfo = deployService.getSnapshotInfo();
        if (snapshotInfo != null) {
            boolean hasChanges = deployService.hasChanges() || true;
            return new GetDeployResponse(true, hasChanges, snapshotInfo.snapshot(), snapshotFilename, snapshotInfo.deltaType());
        }
        return new GetDeployResponse(snapshotFilename);
    }

    record CreateSnapshotRequest(@Nullable DeltaType deltaType) {}

    @Post("/snapshot")
    public void createSnapshot(@Body CreateSnapshotRequest createSnapshotRequest) {
        try {
            deployService.createSnapshot(createSnapshotRequest.deltaType());
        } catch (HttpStatusException e) {
            log.error("createSnapshot failed", e);
            throw Problem.builder()
                    .withTitle(e.getMessage())
                    .withStatus(new HttpStatusType(e.getStatus()))
                    .build();
        }
    }

    @Post("/script/sql")
    public SystemFile scriptSql() {
        File zipFile = deployService.generateSqlScripts();

        return new SystemFile(zipFile)
                .attach("deployment.zip");
    }

    record ScriptFlywayRequest(String name) {}

    record ScriptFlywayResponse(String generatedFile) {}

    @Post("/script/flyway")
    public ScriptFlywayResponse scriptFlyway(@Body ScriptFlywayRequest request) {
        String flywayPath = deployService.generateFlywayScripts(StringUtils.isBlank(request.name()) ? "generated" : request.name());
        return new ScriptFlywayResponse(flywayPath);
    }
}
