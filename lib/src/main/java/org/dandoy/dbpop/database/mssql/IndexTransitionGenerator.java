package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.Transition;
import org.dandoy.dbpop.database.TransitionGenerator;

public class IndexTransitionGenerator extends TransitionGenerator {
    public IndexTransitionGenerator(SqlServerDatabase database) {
        super(database);
    }

    @Override
    protected void drop(ObjectIdentifier objectIdentifier, String fromSql, Transition transition) {
        ObjectIdentifier parent = objectIdentifier.getParent();
        transition.addSql("DROP INDEX %s ON %s".formatted(
                database.quote(objectIdentifier.getName()),
                database.quote(parent)
        ));
    }
}
