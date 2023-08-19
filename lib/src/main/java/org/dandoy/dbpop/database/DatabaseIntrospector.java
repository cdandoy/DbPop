package org.dandoy.dbpop.database;

import java.util.Collection;

public interface DatabaseIntrospector {
    void visit(DatabaseVisitor databaseVisitor);

    void visitModuleMetas(DatabaseVisitor databaseVisitor);

    void visitModuleMetas(String catalog, DatabaseVisitor databaseVisitor);

    void visitModuleDefinitions(DatabaseVisitor databaseVisitor);

    void visitModuleDefinitions(String catalog, DatabaseVisitor databaseVisitor);

    void visitModuleDefinitions(Collection<ObjectIdentifier> objectIdentifiers, DatabaseVisitor databaseVisitor);

    void visitDependencies(DatabaseVisitor databaseVisitor, String catalog);
}
