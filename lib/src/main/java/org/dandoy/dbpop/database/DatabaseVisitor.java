package org.dandoy.dbpop.database;

import java.util.Date;

public interface DatabaseVisitor {
    default void catalog(String catalog) {}

    default void moduleMeta(int objectId, String catalog, String schema, String name, String moduleType, Date modifyDate) {}

    default void moduleDefinition(String catalog, String schema, String name, String moduleType, Date modifyDate, String definition) {}

    default void dependency(String schema, String name, String moduleType, String dependentSchema, String dependentName, String dependentModuleType) {}
}
