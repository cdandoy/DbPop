package org.dandoy.dbpopd.datasets;

import jakarta.inject.Singleton;
import org.dandoy.dbpop.utils.DbPopUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class FileCacheService {
    private final Map<File, FileInfo> fileInfos = new HashMap<>();

    public synchronized FileInfo getFileInfo(File file) {
        FileInfo fileInfo = fileInfos.get(file);
        if (fileInfo == null || fileInfo.lastModified != file.lastModified()) {
            fileInfo = createFileInfo(file);
            fileInfos.put(file, fileInfo);
        }
        return fileInfo;
    }

    private static FileInfo createFileInfo(File file) {
        return new FileInfo(
                file,
                file.lastModified(),
                file.length(),
                DbPopUtils.getCsvRowCount(file)
        );
    }

    public record FileInfo(File file, long lastModified, long length, Integer rowCount) {}
}
