package org.dandoy.dbpop.datasets;

import lombok.Getter;
import org.dandoy.dbpop.upload.DataFile;

public class MissingTablesException extends RuntimeException {
    @Getter
    private final DataFile badDataFile;

    public MissingTablesException(DataFile badDataFile, String message) {
        super(message);
        this.badDataFile = badDataFile;
    }
}
