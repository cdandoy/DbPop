package org.dandoy.dbpop.database.mssql;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Column;
import org.dandoy.dbpop.database.ColumnType;
import org.dandoy.dbpop.database.Database;

@Getter
@Slf4j
public class SqlServerColumn extends Column {
    private final String typeName;
    private final Integer typePrecision;
    private final Integer typeMaxLength;
    private final Integer typeScale;
    private final String seedValue;
    private final String incrementValue;
    private final String defaultConstraintName;
    private final String defaultValue;

    public SqlServerColumn(String column, ColumnType columnType, boolean nullable,
                           String typeName, Integer typePrecision, Integer typeMaxLength, Integer typeScale,
                           String seedValue, String incrementValue,
                           String defaultConstraintName, String defaultValue) {
        super(column, columnType, nullable, seedValue != null);
        this.typeName = typeName;
        this.typePrecision = typePrecision;
        this.typeMaxLength = typeMaxLength;
        this.typeScale = typeScale;
        this.seedValue = seedValue;
        this.incrementValue = incrementValue;
        this.defaultConstraintName = defaultConstraintName;
        this.defaultValue = defaultValue;
    }

    public String toDDL(Database database) {
        return "%s %s%s%s%s".formatted(
                database.quote(getName()),
                getTypeDDL(),
                getIdentityDDL(),
                isNullable() ? "" : " NOT NULL",
                getDefaultConstraint(database)
        );
    }

    private String getDefaultConstraint(Database database) {
        if (defaultConstraintName == null || defaultValue == null) return "";
        return " CONSTRAINT %s DEFAULT %s".formatted(
                database.quote(defaultConstraintName),
                defaultValue
        );
    }

    private String getTypeDDL() {
        return switch (typeName) {
            case "bit" -> "BIT";
            case "tinyint" -> "TINYINT";
            case "smallint" -> "SMALLINT";
            case "int" -> "INT";
            case "bigint" -> "BIGINT";
            case "money" -> "MONEY";
            case "float" -> "FLOAT";
            case "numeric" -> "NUMERIC(%d, %d)".formatted(typePrecision, typeScale);
            case "decimal" -> "DECIMAL(%d, %d)".formatted(typePrecision, typeScale);
            case "varchar" -> "VARCHAR(%s)".formatted(typeMaxLength == -1 ? "MAX" : typeMaxLength);
            case "nvarchar" -> "VARCHAR(%s)".formatted(typeMaxLength == -1 ? "MAX" : typeMaxLength);
            case "char" -> "CHAR(%s)".formatted(typeMaxLength == -1 ? "MAX" : typeMaxLength);
            case "nchar" -> "NCHAR(%s)".formatted(typeMaxLength == -1 ? "MAX" : typeMaxLength);
            case "text" -> "TEXT";
            case "ntext" -> "NTEXT";
            case "time" -> "TIME";
            case "date" -> "DATE";
            case "datetime" -> "DATETIME";
            case "datetime2" -> "DATETIME2";
            case "smalldatetime" -> "SMALLDATETIME";
            case "timestamp" -> "TIMESTAMP";
            case "image" -> "IMAGE";
            case "geography" -> "GEOGRAPHY";
            case "uniqueidentifier" -> "UNIQUEIDENTIFIER";
            case "varbinary" -> "VARBINARY(%s)".formatted(typeMaxLength == -1 ? "MAX" : typeMaxLength);
            default -> {
                log.error("Unknown data type: " + typeName);
                yield "UNKNOWN";
            }
        };
    }

    private String getIdentityDDL() {
        if (seedValue == null) return "";
        return " IDENTITY (%s,%s)".formatted(seedValue, incrementValue);
    }
}
