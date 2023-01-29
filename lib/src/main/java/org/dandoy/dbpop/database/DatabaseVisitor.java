package org.dandoy.dbpop.database;

public interface DatabaseVisitor {
    default void catalog(String catalog) {}
}
