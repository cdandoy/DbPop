package org.dandoy.dbpop;

import java.util.List;
import java.util.Objects;

public class ForeignKey {
    private final String name;
    private final TableName pkTableName;
    private final List<String> pkColumns;
    private final TableName fkTableName;
    private final List<String> fkColumns;

    public ForeignKey(String name, TableName pkTableName, List<String> pkColumns, TableName fkTableName, List<String> fkColumns) {
        this.name = name;
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

    public String getName() {
        return name;
    }

    public TableName getPkTableName() {
        return pkTableName;
    }

    public List<String> getPkColumns() {
        return pkColumns;
    }

    public TableName getFkTableName() {
        return fkTableName;
    }

    public List<String> getFkColumns() {
        return fkColumns;
    }
}
