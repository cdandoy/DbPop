package org.dandoy.dbpop.database;

import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
public class PrimaryKey {
    private final String name;
    private final List<String> columns;

    public PrimaryKey(String name, List<String> columns) {
        this.name = name;
        this.columns = Collections.unmodifiableList(columns);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (PrimaryKey) obj;
        return Objects.equals(this.name, that.name) &&
               Objects.equals(this.columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columns);
    }

    @Override
    public String toString() {
        return "PrimaryKey[" +
               "name=" + name + ", " +
               "columns=" + columns + ']';
    }

    public String toDDL(Database database) {
        return "CONSTRAINT %s PRIMARY KEY (%s)".formatted(
                database.quote(getName()),
                getColumns().stream()
                        .map(database::quote).
                        collect(Collectors.joining(", "))
        );
    }
}
