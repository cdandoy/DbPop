package org.dandoy.dbpopd.code;

import lombok.Getter;
import org.dandoy.dbpop.database.TableName;

@Getter
public final class Dependency implements Comparable<Dependency> {
    private final TableName dependent;
    private final TableName dependsOn;

    public Dependency(TableName dependent, TableName dependsOn) {
        this.dependent = dependent;
        this.dependsOn = dependsOn;
    }

    @Override
    public int compareTo(Dependency that) {
        int i = this.dependent.getCatalog().compareTo(that.dependent.getCatalog());
        if (i != 0) return i;

        i = this.dependent.getSchema().compareTo(that.dependent.getSchema());
        if (i != 0) return i;

        i = this.dependent.getTable().compareTo(that.dependent.getTable());
        if (i != 0) return i;

        i = this.dependsOn.getCatalog().compareTo(that.dependsOn.getCatalog());
        if (i != 0) return i;

        i = this.dependsOn.getSchema().compareTo(that.dependsOn.getSchema());
        if (i != 0) return i;

        i = this.dependsOn.getTable().compareTo(that.dependsOn.getTable());
        return i;
    }
}
