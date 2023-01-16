package org.dandoy.dbpopd.database;

import org.dandoy.dbpop.database.Dependency;
import org.dandoy.dbpop.database.ForeignKey;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;

import java.util.HashSet;
import java.util.Set;

public class DependencyCalculator {
    private final Set<String> processedConstraints = new HashSet<>();
    private final DatabaseService databaseService;

    public DependencyCalculator(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    public static Dependency calculateDependencies(DatabaseService databaseService, Dependency root) {
        DependencyCalculator dependencyCalculator = new DependencyCalculator(databaseService);
        return dependencyCalculator.calculateDependencies(root);
    }

    private Dependency calculateDependencies(Dependency dependency) {
        Dependency ret = Dependency.mutableCopy(dependency);

        if (dependency.isSelected()) {
            TableName tableName = dependency.getTableName();
            Table table = databaseService.getSourceTable(tableName);

            // If we are on invoices, look for customers
            for (ForeignKey foreignKey : table.foreignKeys()) {
                String constraintName = foreignKey.getName();
                if (processedConstraints.add(constraintName)) {
                    Dependency subDependency = dependency
                            .getSubDependencyByConstraint(constraintName)
                            .orElseGet(() -> Dependency.placeHolder(foreignKey.getPkTableName(), constraintName, true));
                    Dependency retSubDependency = calculateDependencies(subDependency);
                    ret.getSubDependencies().add(retSubDependency);
                }
            }

            // If we are on invoices, look for invoice_details
            for (ForeignKey foreignKey : databaseService.getRelatedSourceForeignKeys(tableName)) {
                String constraintName = foreignKey.getName();
                if (processedConstraints.add(constraintName)) {
                    Dependency subDependency = dependency
                            .getSubDependencyByConstraint(constraintName)
                            .orElseGet(() -> Dependency.placeHolder(foreignKey.getFkTableName(), constraintName, false));
                    Dependency retSubDependency = calculateDependencies(subDependency);
                    ret.getSubDependencies().add(retSubDependency);
                }
            }
        }

        return ret;
    }
}
