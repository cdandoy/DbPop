package org.dandoy.dbpop.database;

import lombok.Getter;

@Getter
public abstract class DatabaseVersion {
    private final DatabaseVendor databaseVendor;

    public DatabaseVersion(DatabaseVendor databaseVendor) {
        this.databaseVendor = databaseVendor;
    }
}
