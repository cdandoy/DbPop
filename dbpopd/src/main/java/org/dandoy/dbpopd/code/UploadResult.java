package org.dandoy.dbpopd.code;

import java.util.List;

public record UploadResult(List<FileExecution> fileExecutions, long executionTime) {
    public record FileExecution(String filename, String objectType, String objectName, String error) {}

    public FileExecution getFileExecution(String objectType, String objectName) {
        for (FileExecution fileExecution : fileExecutions) {
            if (objectType.equals(fileExecution.objectType()) && objectName.equals(fileExecution.objectName())) {
                return fileExecution;
            }
        }
        return null;
    }
}
