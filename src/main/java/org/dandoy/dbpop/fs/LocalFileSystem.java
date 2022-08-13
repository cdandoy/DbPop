package org.dandoy.dbpop.fs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class LocalFileSystem extends SimpleFileSystem {
    private final File directory;

    protected LocalFileSystem(File directory, String path) {
        super(path);
        this.directory = directory;
    }

    @Override
    public List<SimpleFileSystem> list() {
        List<SimpleFileSystem> ret = new ArrayList<>();
        listFiles(ret, directory);
        return ret;
    }

    @Override
    public LocalFileSystem cd(String relativePath) {
        return new LocalFileSystem(directory, getPath() + "/" + trimSlashes(relativePath));
    }

    @Override
    public InputStream createInputStream() throws IOException {
        File content = new File(directory, getPath());
        return Files.newInputStream(content.toPath());
    }
}
