package org.dandoy.dbpopd.codechanges;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ObjectIdentifier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;

@Controller("/codechanges/target/")
@Tag(name = "code")
@Slf4j
public class CodeChangeController {
    private final CodeChangeService codeChangeService;
    private final CodeService codeService;

    public CodeChangeController(CodeChangeService codeChangeService, CodeService codeService) {
        this.codeChangeService = codeChangeService;
        this.codeService = codeService;
    }

    @Get
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

    @Post("apply-db")
    public void applyDbChangeToFile(@Body ObjectIdentifier objectIdentifier) {
        CodeChangeService.SignatureDiff signatureDiff = codeChangeService.getSignatureDiff();
        if (signatureDiff.isDifferent(objectIdentifier) || signatureDiff.isDatabaseOnly(objectIdentifier)) {
            codeService.download(objectIdentifier);
        } else if (signatureDiff.isFileOnly(objectIdentifier)) {
            codeService.deleteFile(objectIdentifier);
        }
    }

    @Post("apply-all-db")
    public void applyAllDbChangeToFile() {
        codeChangeService.doWithPause(() -> {
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
            return null;
        });
    }

    @Post("apply-file")
    public ExecutionsResult applyToDatabase(@Body ObjectIdentifier objectIdentifier) {
        return codeChangeService.doWithPause(() -> createExecutionsResult(() -> {
            var signatureDiff = codeChangeService.getSignatureDiff();
            if (signatureDiff.isDifferent(objectIdentifier) || signatureDiff.isFileOnly(objectIdentifier)) {
                return codeService.applyFiles(List.of(objectIdentifier), emptyList());
            } else if (signatureDiff.isDatabaseOnly(objectIdentifier)) {
                return codeService.applyFiles(emptyList(), List.of(objectIdentifier));
            } else {
                return emptyList();
            }
        }));
    }

    @Post("apply-all-files")
    public ExecutionsResult applyAllFileChangeToDb() {
        return codeChangeService.doWithPause(() -> createExecutionsResult(() -> {
            List<ObjectIdentifier> uploadObjectIdentifiers = new ArrayList<>();
            CodeChangeService.SignatureDiff signatureDiff = codeChangeService.getSignatureDiff();

            uploadObjectIdentifiers.addAll(signatureDiff.fileOnly());
            uploadObjectIdentifiers.addAll(signatureDiff.different());
            return codeService.applyFiles(uploadObjectIdentifiers, signatureDiff.databaseOnly());
        }));
    }

    private static ExecutionsResult createExecutionsResult(Supplier<List<ExecutionsResult.Execution>> supplier) {
        long t0 = System.currentTimeMillis();
        List<ExecutionsResult.Execution> executions = supplier.get();
        return new ExecutionsResult(executions, System.currentTimeMillis() - t0);
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
