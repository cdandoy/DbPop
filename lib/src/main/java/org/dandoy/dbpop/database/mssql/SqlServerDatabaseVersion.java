package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.DatabaseVersion;

public class SqlServerDatabaseVersion extends DatabaseVersion {
    static final SqlServerDatabaseVersion BASE_VERSION = new SqlServerDatabaseVersion();

    public SqlServerDatabaseVersion() {
        super(SqlServerDatabaseVendor.INSTANCE);
    }
}
