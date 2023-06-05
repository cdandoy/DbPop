package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.TransitionGenerator;

public class ForeignKeyTransitionGenerator extends TransitionGenerator {
    public ForeignKeyTransitionGenerator(SqlServerDatabase database) {
        super(database);
    }

    @Override
    public String drop(ObjectIdentifier objectIdentifier) {
        ObjectIdentifier parent = objectIdentifier.getParent();
        String fqTableName = database.quote(parent);
        String fqName = objectIdentifier.getName();
        return "ALTER TABLE %s DROP CONSTRAINT %s".formatted(fqTableName, database.quote(fqName));
    }
}
