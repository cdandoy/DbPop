package org.dandoy.dbpop.upload;

import org.dandoy.dbpop.database.TableName;

import java.io.File;

class DataFile {
    private final File file;
    private final TableName tableName;

    public DataFile(File file, TableName tableName) {
        this.file = file;
        this.tableName = tableName;
    }

    public File getFile() {
        return file;
    }

    public TableName getTableName() {
        return tableName;
    }
}
