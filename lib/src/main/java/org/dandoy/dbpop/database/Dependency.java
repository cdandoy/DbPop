package org.dandoy.dbpop.database;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;

@Getter
public final class Dependency {
    private final String displayName;
    private final TableName tableName;
    private final String constraintName;
    private final List<Dependency> subDependencies;
    private final boolean selected;
    private final boolean mandatory;
    private final List<Query> queries;

    @JsonCreator
    public Dependency(
            @JsonProperty("tableName") TableName tableName,
            @JsonProperty("constraintName") String constraintName,
            @JsonProperty("subDependencies") List<Dependency> subDependencies,
            @JsonProperty("selected") boolean selected,
            @JsonProperty("mandatory") boolean mandatory,
            @JsonProperty("queries") List<Query> queries
    ) {
        this.displayName = tableName.toQualifiedName();
        this.tableName = tableName;
        this.constraintName = constraintName;
        this.subDependencies = subDependencies == null ? emptyList() : subDependencies;
        this.selected = selected;
        this.mandatory = mandatory;
        this.queries = queries == null ? emptyList() : queries;
    }

    public static Dependency root(TableName tableName) {
        return new Dependency(tableName, null, emptyList(), true, true, emptyList());
    }

    public static Dependency mutableCopy(Dependency that) {
        return new Dependency(that.tableName, that.getConstraintName(), new ArrayList<>(), that.selected, that.mandatory, emptyList());
    }

    public static Dependency placeHolder(TableName tableName, String constraintName, boolean mandatory) {
        return new Dependency(tableName, constraintName, emptyList(), mandatory, mandatory, emptyList());
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
