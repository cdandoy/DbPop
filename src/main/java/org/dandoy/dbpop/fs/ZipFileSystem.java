package org.dandoy.dbpop.fs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ZipFileSystem extends SimpleFileSystem {
    private final File zipFile;

    protected ZipFileSystem(File zipFile, String path) {
        super(path);
        this.zipFile = zipFile;
    }

    @Override
    public List<SimpleFileSystem> list() {
        List<SimpleFileSystem> ret = new ArrayList<>();
        listZip(ret, zipFile);
        return ret;
    }

    @Override
    public ZipFileSystem cd(String relativePath) {
        return new ZipFileSystem(zipFile, this.getPath() + "/" + trimSlashes(relativePath));
    }

    @Override
    public InputStream createInputStream() throws IOException {
        String fileName = zipFile.getName().toLowerCase();
        if (fileName.endsWith(".jar") || fileName.endsWith(".zip")) {
            InputStream zipInputStream = createZipInputStream(zipFile);
            if (zipInputStream != null) {
                return zipInputStream;
            }
        }
        throw new RuntimeException("Not found: " + this);
    }
}
