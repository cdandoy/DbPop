package org.dandoy.dbpop.database;

import java.util.Collection;

public interface DatabaseIntrospector {
    void visit(DatabaseVisitor databaseVisitor);

    void visitModuleMetas(DatabaseVisitor databaseVisitor, String catalog);

    void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog);

    void visitModuleDefinitions(DatabaseVisitor databaseVisitor, Collection<ObjectIdentifier> objectIdentifiers);

    void visitDependencies(DatabaseVisitor databaseVisitor, String catalog);
}
