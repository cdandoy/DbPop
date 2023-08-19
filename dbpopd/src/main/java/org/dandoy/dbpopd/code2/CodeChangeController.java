package org.dandoy.dbpopd.code2;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.config.ConfigurationService;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

@Controller("/code2")
@Tag(name = "code")
@Slf4j
public class CodeChangeController {
    private final CodeChangeService codeChangeService;
    private final CodeService codeService;

    public CodeChangeController(CodeChangeService codeChangeService, CodeService codeService) {
        this.codeChangeService = codeChangeService;
        this.codeService = codeService;
    }

    @Get("target/changes")
    public Stream<ChangedObject> targetChanges() {
        CodeChangeService.SignatureDiff signatureDiff = codeChangeService.getSignatureDiff();
        return Stream
                .of(
                        signatureDiff.fileOnly().stream().map(it -> new ChangedObject(it, ChangeType.FILE_ONLY)),
                        signatureDiff.databaseOnly().stream().map(it -> new ChangedObject(it, ChangeType.DATABASE_ONLY)),
                        signatureDiff.different().stream().map(it -> new ChangedObject(it, ChangeType.UPDATED))
                )
                .flatMap(identity())
                .sorted(Comparator.comparing(changedObject -> changedObject.objectIdentifier.toQualifiedName()))
                .limit(100);
    }

    @Post("target/changes/apply-db")
    public void applyDbChangeToFile(@Body ObjectIdentifier objectIdentifier) {
        CodeChangeService.SignatureDiff signatureDiff = codeChangeService.getSignatureDiff();
        if (signatureDiff.isDifferent(objectIdentifier) || signatureDiff.isDatabaseOnly(objectIdentifier)) {
            codeService.download(objectIdentifier);
        } else if (signatureDiff.isFileOnly(objectIdentifier)) {
            codeService.deleteFile(objectIdentifier);
        }
    }

    @Post("target/changes/apply-all-db")
    public void applyAllDbChangeToFile() {
        CodeChangeService.SignatureDiff signatureDiff = codeChangeService.getSignatureDiff();
        for (ObjectIdentifier objectIdentifier : signatureDiff.databaseOnly()) {
            codeService.download(objectIdentifier);
        }
        for (ObjectIdentifier objectIdentifier : signatureDiff.different()) {
            codeService.download(objectIdentifier);
        }
        for (ObjectIdentifier objectIdentifier : signatureDiff.fileOnly()) {
            codeService.deleteFile(objectIdentifier);
        }
    }

    @Post("target/changes/apply-file")
    public ExecutionsResult applyToDatabase(@Body ObjectIdentifier objectIdentifier) {
        long t0 = System.currentTimeMillis();
        CodeChangeService.SignatureDiff signatureDiff = codeChangeService.getSignatureDiff();
        if (signatureDiff.isDifferent(objectIdentifier) || signatureDiff.isFileOnly(objectIdentifier)) {
            ExecutionsResult.Execution execution = codeService.uploadFileToTarget(objectIdentifier);
            return new ExecutionsResult(List.of(execution), System.currentTimeMillis() - t0);
        } else if (signatureDiff.isDatabaseOnly(objectIdentifier)) {
            ExecutionsResult.Execution execution = codeService.drop(objectIdentifier);
            return new ExecutionsResult(List.of(execution), System.currentTimeMillis() - t0);
        } else {
            return new ExecutionsResult(Collections.emptyList(), 0);
        }
    }

    @Post("target/changes/apply-all-files")
    public void applyAllFileChangeToDb() {
        CodeChangeService.SignatureDiff signatureDiff = codeChangeService.getSignatureDiff();
        for (ObjectIdentifier objectIdentifier : signatureDiff.fileOnly()) {
            codeService.uploadFileToTarget(objectIdentifier);
        }
        for (ObjectIdentifier objectIdentifier : signatureDiff.different()) {
            codeService.uploadFileToTarget(objectIdentifier);
        }
        for (ObjectIdentifier objectIdentifier : signatureDiff.databaseOnly()) {
            codeService.drop(objectIdentifier);
        }
    }

    public enum ChangeType {
        FILE_ONLY,
        DATABASE_ONLY,
        UPDATED,
    }

    public record ChangedObject(ObjectIdentifier objectIdentifier, String name, String type, ChangeType changeType) {
        public ChangedObject(ObjectIdentifier objectIdentifier, ChangeType changeType) {
            this(
                    objectIdentifier,
                    objectIdentifier.toQualifiedName(),
                    objectIdentifier.getType(),
                    changeType);
        }
    }
}
