package org.dandoy.dbpop.database;

import java.util.Date;

public interface DatabaseVisitor {
    default void catalog(String catalog) {}

    default void moduleMeta(ObjectIdentifier objectIdentifier, Date modifyDate) {}

    default void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {}

    default void dependency(String catalog, String schema, String name, String moduleType, String dependentSchema, String dependentName, String dependentModuleType) {}
}
