package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.Database;

public class SqlServerType {
    private final String schema;
    private final String typeName;
    private final String baseTypeName;
    private final Integer typePrecision;
    private final Integer typeMaxLength;
    private final Integer typeScale;
    private final boolean nullable;

    public SqlServerType(String schema, String typeName, String baseTypeName, Integer typePrecision, Integer typeMaxLength, Integer typeScale, boolean nullable) {
        this.schema = schema;
        this.typeName = typeName;
        this.baseTypeName = baseTypeName;
        this.typePrecision = typePrecision;
        this.typeMaxLength = typeMaxLength;
        this.typeScale = typeScale;
        this.nullable = nullable;
    }

    public String toDDL(Database database) {
        return "CREATE TYPE %s.%s FROM %s%s".formatted(
                database.quote(schema),
                database.quote(typeName),
                SqlServerColumn.getTypeDDL(
                        database, baseTypeName, typePrecision, typeMaxLength, typeScale
                ),
                nullable ? "" : " NOT NULL"
        );
    }
}
