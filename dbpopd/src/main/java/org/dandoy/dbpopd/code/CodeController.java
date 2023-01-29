package org.dandoy.dbpopd.code;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

@Controller("/code")
public class CodeController {
    private final CodeService codeService;

    public CodeController(CodeService codeService) {
        this.codeService = codeService;
    }

    @Get("source-to-file")
    public void sourceToFile() {
        codeService.sourceToFile();
    }

    @Get("source-to-file")
    public void fileToTarget() {
        codeService.fileToTarget();
    }
}
