package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.TransitionGenerator;

public class PrimaryKeyTransitionGenerator extends TransitionGenerator {
    public PrimaryKeyTransitionGenerator(SqlServerDatabase database) {
        super(database);
    }

    @Override
    public String drop(ObjectIdentifier objectIdentifier) {
        return "ALTER TABLE %s DROP CONSTRAINT %s".formatted(
                database.quote(objectIdentifier.getParent()),
                database.quote(objectIdentifier.getName())
        );
    }
}
