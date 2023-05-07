package org.dandoy.dbpop.database.mssql;

import lombok.Getter;
import org.dandoy.dbpop.database.ObjectIdentifier;

@Getter
public class SqlServerObjectIdentifier extends ObjectIdentifier {
    private final Integer objectId;

    public SqlServerObjectIdentifier(Integer objectId, String type, String catalog, String schema, String name, ObjectIdentifier parent) {
        super(type, catalog, schema, name, parent);
        this.objectId = objectId;
    }

    public SqlServerObjectIdentifier(Integer objectId, String type, String catalog, String schema, String name) {
        super(type, catalog, schema, name);
        this.objectId = objectId;
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
