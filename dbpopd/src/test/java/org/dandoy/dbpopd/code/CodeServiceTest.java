package org.dandoy.dbpopd.code;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
@MicronautTest
class CodeServiceTest {
    @Inject
    CodeService codeService;

    @Test
    void testFileToTarget() {
        codeService.uploadFileToTarget();
    }

    @Test
    void testDownloadSourceToFile() {
        codeService.downloadSourceToFile();
    }

    @Test
    void testCompareSourceToFile() {
        codeService.compareSourceToFile();
    }
}