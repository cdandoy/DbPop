package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.DatabaseVisitor;

import java.sql.Date;

@SuppressWarnings("unused")
public interface SqlServerDatabaseVisitor extends DatabaseVisitor {
    default void moduleMeta(String catalog, String schema, String name, String moduleType, Date modifyDate) {}

    default void moduleDefinition(String catalog, String schema, String name, String moduleType, Date modifyDate, String definition) {}

    default void dependency(String schema, String name, String moduleType, String dependentSchema, String dependentName, String dependentModuleType) {}
}
