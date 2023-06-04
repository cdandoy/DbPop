package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.Transition;
import org.dandoy.dbpop.database.TransitionGenerator;

public class TableTransitionGenerator extends TransitionGenerator {
    public TableTransitionGenerator(Database database) {
        super(database);
    }

    @Override
    protected void drop(ObjectIdentifier objectIdentifier, String fromSql, Transition transition) {
        String sql = "DROP TABLE %s".formatted(
                database.quote(
                        objectIdentifier.getCatalog(),
                        objectIdentifier.getSchema(),
                        objectIdentifier.getName()
                )
        );
        transition.addSql(sql);
    }

    @Override
    protected void update(ObjectIdentifier objectIdentifier, String fromSql, String toSql, Transition transition) {
        transition.setError("Cannot ALTER tables yet: %s", objectIdentifier.toQualifiedName());
    }
}
