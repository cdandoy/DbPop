package org.dandoy.dbpop;

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
