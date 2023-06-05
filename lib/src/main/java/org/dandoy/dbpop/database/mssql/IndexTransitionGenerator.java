package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.TransitionGenerator;

public class IndexTransitionGenerator extends TransitionGenerator {
    public IndexTransitionGenerator(SqlServerDatabase database) {
        super(database);
    }

    @Override
    public String drop(ObjectIdentifier objectIdentifier) {
        ObjectIdentifier parent = objectIdentifier.getParent();
        return "DROP INDEX %s ON %s".formatted(
                database.quote(objectIdentifier.getName()),
                database.quote(parent)
        );
    }
}
