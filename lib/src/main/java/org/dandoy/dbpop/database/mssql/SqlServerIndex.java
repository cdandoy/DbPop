package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.Index;
import org.dandoy.dbpop.database.TableName;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

public class SqlServerIndex extends Index {
    public enum UsingXmlIndexForType {PATH, PROPERTY, VALUE}

    private final String typeDesc;
    private final String xmlTypeDesc;
    private final String usingXmlIndexName;
    private final UsingXmlIndexForType usingXmlIndexForType;
    private final List<SqlServerIndexColumn> columns;

    public SqlServerIndex(String name, TableName tableName, boolean unique, boolean primaryKey, String typeDesc,
                          String xmlTypeDesc, String usingXmlIndexName, UsingXmlIndexForType usingXmlIndexForType,
                          List<SqlServerIndexColumn> columns) {
        super(name, tableName, unique, primaryKey, getIndexedColumns(columns));
        this.typeDesc = typeDesc;
        this.xmlTypeDesc = xmlTypeDesc;
        this.usingXmlIndexName = usingXmlIndexName;
        this.usingXmlIndexForType = usingXmlIndexForType;
        this.columns = columns;
    }

    @NotNull
    public static List<String> getIndexedColumns(List<SqlServerIndexColumn> sqlServerIndexColumns) {
        return sqlServerIndexColumns.stream()
                .filter(it -> !it.included())
                .map(it -> it.name)
                .toList();
    }

    public String toDDL(Database database) {
        List<String> includedColumnNames = columns.stream()
                .filter(SqlServerIndexColumn::included)
                .map(it -> database.quote(it.name))
                .toList();
        if (!isPrimaryKey()) {
            return "CREATE%s%s INDEX %s ON %s (%s)%s%s"
                    .formatted(
                            isUnique() ? " UNIQUE" : "",
                            getIndexType(),
                            database.quote(getName()),
                            database.quote(getTableName()),
                            super.getColumns().stream().map(database::quote).collect(Collectors.joining(", ")),
                            includedColumnNames.isEmpty() ? "" : " INCLUDE (" + String.join(", ", includedColumnNames) + ")",
                            getUsingXml(database)
                    );
        } else {
            return "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY %s (%s)".formatted(
                    database.quote(getTableName()),
                    database.quote(getName()),
                    getIndexType(),
                    super.getColumns().stream().map(database::quote).collect(Collectors.joining(", "))
            );
        }
    }

    private String getUsingXml(Database database) {
        if (usingXmlIndexName == null) return "";
        return " USING XML INDEX %s FOR %s".formatted(database.quote(usingXmlIndexName), usingXmlIndexForType);
    }

    private String getIndexType() {
        if ("XML".equals(typeDesc)) {
            if ("PRIMARY_XML".equals(xmlTypeDesc)) {
                return " PRIMARY XML";
            } else {
                return " XML";
            }
        } else {
            return " " + typeDesc;
        }
    }

    public record SqlServerIndexColumn(String name, boolean included) {}
}
