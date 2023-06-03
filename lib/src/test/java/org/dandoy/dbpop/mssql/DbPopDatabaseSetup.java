package org.dandoy.dbpop.mssql;

import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.tests.mssql.DbPopContainerSetup;
import org.testcontainers.containers.MSSQLServerContainer;

public class DbPopDatabaseSetup {
    public static Database getSourceDatabase() {
        MSSQLServerContainer<?> sourceContainer = DbPopContainerSetup.getSourceContainer();
        ConnectionBuilder connectionBuilder = new UrlConnectionBuilder(
                sourceContainer.getJdbcUrl(),
                sourceContainer.getUsername(),
                sourceContainer.getPassword()
        );
        return Database.createDatabase(connectionBuilder);
    }

    public static Database getTargetDatabase() {
        MSSQLServerContainer<?> targetContainer = DbPopContainerSetup.getTargetContainer();
        ConnectionBuilder connectionBuilder = new UrlConnectionBuilder(
                targetContainer.getJdbcUrl(),
                targetContainer.getUsername(),
                targetContainer.getPassword()
        );
        return Database.createDatabase(connectionBuilder);
    }
}
