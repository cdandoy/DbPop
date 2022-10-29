package org.dandoy.dbpop.upload;

import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpop.fs.SimpleFileSystem;

import java.io.IOException;
import java.io.InputStream;

public class DataFile {
    private final SimpleFileSystem simpleFileSystem;
    private final TableName tableName;

    public DataFile(SimpleFileSystem simpleFileSystem, TableName tableName) {
        this.simpleFileSystem = simpleFileSystem;
        this.tableName = tableName;
    }

    public InputStream createInputStream() throws IOException {
        return simpleFileSystem.createInputStream();
    }

    public SimpleFileSystem getSimpleFileSystem() {
        return simpleFileSystem;
    }

    public TableName getTableName() {
        return tableName;
    }
}
