package org.dandoy.dbpop.database;

import java.sql.Timestamp;
import java.util.Collection;

public interface DatabaseIntrospector {
    void visit(DatabaseVisitor databaseVisitor);

    void visitModuleDefinitions(DatabaseVisitor databaseVisitor, Timestamp since);

    void visitModuleMetas(DatabaseVisitor databaseVisitor, String catalog);

    void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog);

    void visitModuleDefinitions(DatabaseVisitor databaseVisitor, Collection<ObjectIdentifier> objectIdentifiers);

    void visitDependencies(DatabaseVisitor databaseVisitor, String catalog);
}
