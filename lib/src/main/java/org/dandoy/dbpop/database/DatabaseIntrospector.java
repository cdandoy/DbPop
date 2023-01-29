package org.dandoy.dbpop.database;

public interface DatabaseIntrospector {
    void visit(DatabaseVisitor databaseVisitor);

    void visitModuleMetas(DatabaseVisitor databaseVisitor, String catalog);

    void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog);

    void visitDependencies(DatabaseVisitor databaseVisitor, String catalog);
}