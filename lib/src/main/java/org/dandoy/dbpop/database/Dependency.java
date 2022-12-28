package org.dandoy.dbpop.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;

public record Dependency(TableName tableName, String constraintName, List<Dependency> subDependencies, boolean selected, boolean mandatory) {

    public static Dependency root(TableName tableName) {
        return new Dependency(tableName, null, emptyList(), true, true);
    }

    public static Dependency mutableCopy(Dependency that) {
        return new Dependency(that.tableName, that.constraintName(), new ArrayList<>(), that.selected, that.mandatory);
    }

    public static Dependency placeHolder(TableName tableName, String constraintName, boolean mandatory) {
        return new Dependency(tableName, constraintName, emptyList(), mandatory, mandatory);
    }

    Optional<Dependency> getSubDependencyByConstraint(String constraintName) {
        for (Dependency subDependency : subDependencies) {
            if (constraintName.equals(subDependency.constraintName)) {
                return Optional.of(subDependency);
            }
        }
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "Dependency[" +
               "tableName=" + tableName + ", " +
               "constraintName=" + constraintName + ", " +
               "subDependencies=" + subDependencies + ", " +
               "selected=" + selected + ']';
    }

    @SuppressWarnings("unused")
    public String dump() {
        return dump("");
    }

    private String dump(String indent) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(indent);
        stringBuilder.append(tableName.getTable());
        if (mandatory) stringBuilder.append(" mandatory");
        if (selected) stringBuilder.append(" selected");
        stringBuilder.append("\n");

        for (Dependency subDependency : subDependencies) {
            stringBuilder.append(subDependency.dump("    " + indent));
        }
        return stringBuilder.toString();
    }
}
