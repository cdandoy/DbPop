package org.dandoy.dbpop.upload;

import org.dandoy.dbpop.database.TableName;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

public class DataFile {
    private final File file;
    private final TableName tableName;

    public DataFile(File file, TableName tableName) {
        this.file = file;
        this.tableName = tableName;
    }

    public InputStream createInputStream() throws IOException {
        return Files.newInputStream(file.toPath());
    }

    public File getFile() {
        return file;
    }

    public TableName getTableName() {
        return tableName;
    }
}
