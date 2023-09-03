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
import org.dandoy.dbpop.utils.StringUtils;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.DatabaseCacheService;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;
import org.dandoy.dbpopd.utils.IOUtils;
import org.dandoy.diff.DiffLine;
import org.dandoy.diff.DiffLineGenerator;

import java.io.File;
import java.util.Arrays;
import java.util.List;

@Controller("/code")
@Tag(name = "code")
@Slf4j
public class CodeController {
    private final ConfigurationService configurationService;
    private final DatabaseCacheService databaseCacheService;
    private final CodeService codeService;

    public CodeController(ConfigurationService configurationService, DatabaseCacheService databaseCacheService, CodeService codeService) {
        this.configurationService = configurationService;
        this.databaseCacheService = databaseCacheService;
        this.codeService = codeService;
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

    public record CodeDiffResponse(List<DiffLine> diffLines, String leftName, String rightName) {}

    @Post("/target/diff/")
    public CodeDiffResponse targetDiff(@Body ObjectIdentifier objectIdentifier) {
        String leftDefinition = getFileDefinition(objectIdentifier);
        String rightDefinition = getDatabaseDefinition(objectIdentifier);
        List<String> fileLines = Arrays.asList(leftDefinition.split("\n"));
        List<String> databaseLines = Arrays.asList(rightDefinition.split("\n"));
        Patch<String> patch = DiffUtils.diff(
                fileLines,
                databaseLines,
                DiffRowGenerator.DEFAULT_EQUALIZER
//                DiffRowGenerator.IGNORE_WHITESPACE_EQUALIZER
        );

        return new CodeDiffResponse(
                DiffLineGenerator.create()
                        .showInlineDiffs(true)
                        .inlineDiffByWord(true)
                        .build()
                        .generateDiffLines(fileLines, patch),
                DbPopdFileUtils.encodeFileName(objectIdentifier),
                objectIdentifier.toQualifiedName()
        );
    }

    private String getFileDefinition(ObjectIdentifier objectIdentifier) {
        File file = DbPopdFileUtils.toFile(configurationService.getCodeDirectory(), objectIdentifier);
        if (file != null && file.isFile()) {
            return StringUtils.normalizeEOL(IOUtils.toString(file));
        }
        return "";
    }

    private String getDatabaseDefinition(ObjectIdentifier objectIdentifier) {
        Database targetDatabase = databaseCacheService.getTargetDatabaseCacheOrThrow();
        String definition = targetDatabase.getDefinition(objectIdentifier);
        if (definition == null) return "";
        return StringUtils.normalizeEOL(definition);
    }
}
