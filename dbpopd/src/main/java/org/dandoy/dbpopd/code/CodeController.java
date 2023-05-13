package org.dandoy.dbpopd.code;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.dandoy.dbpopd.ConfigurationService;

import java.nio.file.Path;
import java.util.stream.Stream;

@Controller("/code")
@Tag(name = "code")
public class CodeController {
    private final ConfigurationService configurationService;
    private final CodeService codeService;
    private final ChangeDetector changeDetector;

    public CodeController(ConfigurationService configurationService, CodeService codeService, ChangeDetector changeDetector) {
        this.configurationService = configurationService;
        this.codeService = codeService;
        this.changeDetector = changeDetector;
    }

    @Get("source/compare")
    public CodeDiff compareSourceToFile() {
        return codeService.compareSourceToFile();
    }

    @Get("source/download")
    public DownloadResult downloadSourceToFile() {
        return codeService.downloadSourceToFile();
    }

    @Get("target/compare")
    public CodeDiff compareTargetToFile() {
        return codeService.compareTargetToFile();
    }

    @Get("target/upload")
    public UploadResult uploadFileToTarget() {
        return codeService.uploadFileToTarget();
    }

    @Get("target/download")
    public DownloadResult downloadTargetToFile() {
        return codeService.downloadTargetToFile();
    }

    @Get("target/changes")
    public Stream<ChangeResponse> targetChanges() {
        Path codePath = configurationService.getCodeDirectory().toPath();
        return changeDetector.getChanges()
                .stream()
                .map(change -> {
                            Path path = codePath.relativize(change.getFile().toPath());
                            return new ChangeResponse(
                                    path.toString(),
                                    change.getObjectIdentifier().toQualifiedName(),
                                    change.isFileChanged(),
                                    change.isDatabaseChanged()
                            );
                        }
                );
    }

    record ChangeResponse(String path, String dbname, boolean fileChanged, boolean databaseChanged) {}
}
