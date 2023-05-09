package org.dandoy.dbpop.database.mssql;

import lombok.Getter;
import org.dandoy.dbpop.database.ObjectIdentifier;

@Getter
public class SqlServerObjectIdentifier extends ObjectIdentifier {
    private final Integer objectId;

    public SqlServerObjectIdentifier(Integer objectId, String type, String catalog, String schema, String name, SqlServerObjectIdentifier parent) {
        super(type, catalog, schema, name, parent);
        if ("INDEX".equals(type) && objectId != null) throw new RuntimeException("Indexes have no objectIds in SQL Server");
        this.objectId = objectId;
    }

    public SqlServerObjectIdentifier(Integer objectId, String type, String catalog, String schema, String name) {
        super(type, catalog, schema, name);
        if ("FOREIGN_KEY_CONSTRAINT".equals(type) || "INDEX".equals(type)) throw new RuntimeException("Foreign Keys and Indexes must have a parent");

        this.objectId = objectId;
    }

    @Override
    public SqlServerObjectIdentifier getParent() {
        return (SqlServerObjectIdentifier) super.getParent();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof SqlServerObjectIdentifier that) {
            if (this.objectId != null) {
                if (this.objectId.equals(that.objectId)) {
                    return true;
                }
            }
        }
        return super.equals(o);
    }

    @Override
    public String toString() {
        return super.toString() + " - " + objectId;
    }
}
