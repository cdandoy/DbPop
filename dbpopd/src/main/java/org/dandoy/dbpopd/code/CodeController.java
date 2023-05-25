package org.dandoy.dbpopd.code;

import com.github.difflib.DiffUtils;
import com.github.difflib.patch.Patch;
import com.github.difflib.text.DiffRowGenerator;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;
import org.dandoy.dbpopd.utils.IOUtils;
import org.dandoy.diff.DiffLine;
import org.dandoy.diff.DiffLineGenerator;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@Controller("/code")
@Tag(name = "code")
@Slf4j
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

    record ChangeResponse(String path, String dbname, ObjectIdentifierResponse objectIdentifier, boolean fileChanged, boolean databaseChanged, boolean fileDeleted, boolean databaseDeleted) {}

    record ObjectIdentifierResponse(String type, TableName tableName, ObjectIdentifierResponse parent) {
        static ObjectIdentifierResponse toObjectIdentifierResponse(ObjectIdentifier objectIdentifier) {
            return new ObjectIdentifierResponse(
                    objectIdentifier.getType(),
                    new TableName(
                            objectIdentifier.getCatalog(),
                            objectIdentifier.getSchema(),
                            objectIdentifier.getName()
                    ),
                    objectIdentifier.getParent() == null ? null : toObjectIdentifierResponse(objectIdentifier.getParent())
            );
        }

        public ObjectIdentifier toObjectIdentifier() {
            return new ObjectIdentifier(
                    type, tableName().getCatalog(), tableName().getSchema(), tableName.getTable(),
                    parent == null ? null : parent.toObjectIdentifier()
            );
        }
    }

    @Get("target/changes")
    public Stream<ChangeResponse> targetChanges() {
        Path codePath = configurationService.getCodeDirectory().toPath();
        return changeDetector.getChanges()
                .stream()
                .map(change -> {
                            String path = change.getFile() == null ? null : codePath.relativize(change.getFile().toPath()).toString();
                            ObjectIdentifier objectIdentifier = change.getObjectIdentifier();
                            return new ChangeResponse(
                                    path,
                                    objectIdentifier == null ? null : objectIdentifier.toQualifiedName(),
                                    objectIdentifier == null ? null : ObjectIdentifierResponse.toObjectIdentifierResponse(objectIdentifier),
                                    change.isFileChanged(),
                                    change.isDatabaseChanged(),
                                    change.isFileDeleted(),
                                    change.isDatabaseDeleted()
                            );
                        }
                )
                .sorted(Comparator.comparing(ChangeResponse::path));
    }

    @Post("target/changes/apply-files")
    public void applyToDatabase(@Body ObjectIdentifierResponse[] objectIdentifierResponses) {
        for (ObjectIdentifierResponse objectIdentifierResponse : objectIdentifierResponses) {
            ObjectIdentifier objectIdentifier = objectIdentifierResponse.toObjectIdentifier();
            Change change = changeDetector.removeChange(objectIdentifier);
            File file = change.getFile();
            if (change.isFileChanged()) {
                codeService.uploadFileToTarget(file);
            } else if (change.isFileDeleted()) {
                codeService.deleteTargetObject(objectIdentifier);
            } else {
                throw new RuntimeException();
            }
        }
    }

    @Post("target/changes/apply-dbs")
    public void applyToFile(@Body ObjectIdentifierResponse[] objectIdentifierResponses) {
        for (ObjectIdentifierResponse objectIdentifierResponse : objectIdentifierResponses) {
            ObjectIdentifier objectIdentifier = objectIdentifierResponse.toObjectIdentifier();
            Change change = changeDetector.removeChange(objectIdentifier);
            File file = change.getFile();
            if (change.isDatabaseChanged()) {
                codeService.downloadTargetToFile(objectIdentifier);
            } else if (change.isDatabaseDeleted()) {
                codeService.deleteFile(file);
            } else {
                throw new RuntimeException();
            }
        }
    }

    public record CodeDiffResponse(List<DiffLine> diffLines, String leftName, String rightName) {}

    @Post("/target/diff/")
    public CodeDiffResponse targetDiff(@Body ObjectIdentifierResponse objectIdentifierResponse) {
        ObjectIdentifier objectIdentifier = objectIdentifierResponse.toObjectIdentifier();
        String leftDefinition = getFileDefinition(objectIdentifier);
        String rightDefinition = getDatabaseDefinition(objectIdentifier);
//        String leftDefinition = getFileDefinition(new ObjectIdentifier("SQL_STORED_PROCEDURE", "master", "dbo", "GetInvoices"));
//        String rightDefinition = getFileDefinition(new ObjectIdentifier("SQL_STORED_PROCEDURE", "master", "dbo", "GetInvoices2"));
        List<String> fileLines = Arrays.asList(leftDefinition.split("\n"));
        List<String> databaseLines = Arrays.asList(rightDefinition.split("\n"));
        Patch<String> patch = DiffUtils.diff(
                fileLines,
                databaseLines,
                DiffRowGenerator.DEFAULT_EQUALIZER
        );

        return new CodeDiffResponse(
                DiffLineGenerator.create()
                        .showInlineDiffs(true)
                        .inlineDiffByWord(true)
                        .build()
                        .generateDiffLines(fileLines, patch),
                DbPopdFileUtils.toFileName(objectIdentifier),
                objectIdentifier.toQualifiedName()
        );
    }

    private String getFileDefinition(ObjectIdentifier objectIdentifier) {
        File file = DbPopdFileUtils.toFile(configurationService.getCodeDirectory(), objectIdentifier);
        if (file != null && file.isFile()) {
            return IOUtils.toString(file)
                    .replace("\r\n", "\n")  // Windows
                    .replace("\r", "\n");   // Mac
        }
        return "";
    }

    private String getDatabaseDefinition(ObjectIdentifier objectIdentifier) {
        Database targetDatabase = configurationService.getTargetDatabaseCache();
        String definition = targetDatabase.getDefinition(objectIdentifier);
        if (definition == null) return "";
        return definition;
    }
}
