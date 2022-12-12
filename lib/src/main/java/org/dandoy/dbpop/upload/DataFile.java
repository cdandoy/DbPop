package org.dandoy.dbpop.upload;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.dandoy.dbpop.database.TableName;

import java.io.File;

@Getter
@AllArgsConstructor
public class DataFile {
    private final File file;
    private final TableName tableName;
}
