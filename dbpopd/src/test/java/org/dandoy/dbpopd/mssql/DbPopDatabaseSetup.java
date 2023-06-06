package org.dandoy.dbpopd.mssql;

import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.database.mssql.SqlServerDatabase;
import org.dandoy.dbpop.tests.mssql.DbPopContainerSetup;
import org.testcontainers.containers.MSSQLServerContainer;

public class DbPopDatabaseSetup {

    public static SqlServerDatabase getTargetDatabase() {
        MSSQLServerContainer<?> targetContainer = DbPopContainerSetup.getTargetContainer();
        ConnectionBuilder connectionBuilder = new UrlConnectionBuilder(
                targetContainer.getJdbcUrl(),
                targetContainer.getUsername(),
                targetContainer.getPassword()
        );
        return new SqlServerDatabase(connectionBuilder);
    }
}
