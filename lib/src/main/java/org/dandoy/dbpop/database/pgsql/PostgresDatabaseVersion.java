package org.dandoy.dbpop.database.pgsql;

import org.dandoy.dbpop.database.DatabaseVersion;

public class PostgresDatabaseVersion extends DatabaseVersion {
    static final PostgresDatabaseVersion BASE_VERSION = new PostgresDatabaseVersion();

    public PostgresDatabaseVersion() {
        super(PostgresDatabaseVendor.INSTANCE);
    }
}
