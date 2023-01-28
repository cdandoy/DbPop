package org.dandoy.dbpop.database;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
public class ForeignKey {
    private final String name;
    private final String constraintDef;
    private final TableName pkTableName;
    private final List<String> pkColumns;
    private final TableName fkTableName;
    private final List<String> fkColumns;

    @JsonCreator
    public ForeignKey(
            @JsonProperty("name") String name,
            @JsonProperty("constraintDef") String constraintDef,
            @JsonProperty("kTableName") TableName pkTableName,
            @JsonProperty("pkColumns") List<String> pkColumns,
            @JsonProperty("fkTableName") TableName fkTableName,
            @JsonProperty("fkColumns") List<String> fkColumns) {
        this.name = name;
        this.constraintDef = constraintDef;
        this.pkTableName = pkTableName;
        this.pkColumns = pkColumns;
        this.fkTableName = fkTableName;
        this.fkColumns = fkColumns;
    }

    @Override
    public String toString() {
        return "ForeignKey{" +
               "name='" + name + '\'' +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForeignKey that = (ForeignKey) o;
        return Objects.equals(name, that.name) && pkTableName.equals(that.pkTableName) && pkColumns.equals(that.pkColumns) && fkTableName.equals(that.fkTableName) && fkColumns.equals(that.fkColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    public String toDDL(Database database) {
        return "CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)".formatted(
                database.quote(getName()),
                getFkColumns().stream().map(database::quote).collect(Collectors.joining(", ")),
                database.quote(getPkTableName()),
                getPkColumns().stream().map(database::quote).collect(Collectors.joining(", "))
        );
    }
}
