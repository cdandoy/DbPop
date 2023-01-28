package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.DatabaseVisitor;

import java.sql.Date;

@SuppressWarnings("unused")
public class SqlServerDatabaseVisitor extends DatabaseVisitor {
    public void moduleMeta(String catalog, String schema, String name, String moduleType, Date modifyDate) {}

    public void moduleDefinition(String catalog, String schema, String name, String moduleType, Date modifyDate, String definition) {}

    public void dependency(String schema, String name, String moduleType, String dependentSchema, String dependentName, String dependentModuleType) {}
}
