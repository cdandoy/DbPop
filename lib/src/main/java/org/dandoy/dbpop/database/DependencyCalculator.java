package org.dandoy.dbpop.database;

import java.util.HashSet;
import java.util.Set;

public class DependencyCalculator {
    private final Database database;
    private final Set<String> processedConstraints = new HashSet<>();

    public DependencyCalculator(Database database) {
        this.database = database;
    }

    public static Dependency calculateDependencies(Database database, Dependency root) {
        DependencyCalculator dependencyCalculator = new DependencyCalculator(database);
        return dependencyCalculator.calculateDependencies(root);
    }

    private Dependency calculateDependencies(Dependency dependency) {
        Dependency ret = Dependency.mutableCopy(dependency);

        if (dependency.selected()) {
            Table table = database.getTable(dependency.tableName());

            // If we are on invoices, look for customers
            for (ForeignKey foreignKey : table.foreignKeys()) {
                String constraintName = foreignKey.getName();
                if (processedConstraints.add(constraintName)) {
                    Dependency subDependency = dependency
                            .getSubDependencyByConstraint(constraintName)
                            .orElseGet(() -> Dependency.placeHolder(foreignKey.getPkTableName(), constraintName, true));
                    Dependency retSubDependency = calculateDependencies(subDependency);
                    ret.subDependencies().add(retSubDependency);
                }
            }

            // If we are on invoices, look for invoice_details
            for (ForeignKey foreignKey : database.getRelatedForeignKeys(dependency.tableName())) {
                String constraintName = foreignKey.getName();
                if (processedConstraints.add(constraintName)) {
                    Dependency subDependency = dependency
                            .getSubDependencyByConstraint(constraintName)
                            .orElseGet(() -> Dependency.placeHolder(foreignKey.getFkTableName(), constraintName, false));
                    Dependency retSubDependency = calculateDependencies(subDependency);
                    ret.subDependencies().add(retSubDependency);
                }
            }
        }

        return ret;
    }
}
