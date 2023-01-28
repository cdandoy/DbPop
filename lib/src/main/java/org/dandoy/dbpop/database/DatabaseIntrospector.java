package org.dandoy.dbpop.database;

public interface DatabaseIntrospector {
    void visit(DatabaseVisitor databaseVisitor);
}
