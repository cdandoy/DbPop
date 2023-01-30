package org.dandoy.dbpopd.code;

import java.util.List;

public record UploadResult(List<FileExecution> fileExecutions, long executionTime) {
    public record FileExecution(String filename, String error) {}
}
