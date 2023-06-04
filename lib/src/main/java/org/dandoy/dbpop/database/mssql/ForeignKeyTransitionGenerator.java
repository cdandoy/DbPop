package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.Transition;
import org.dandoy.dbpop.database.TransitionGenerator;

public class ForeignKeyTransitionGenerator extends TransitionGenerator {
    public ForeignKeyTransitionGenerator(Database database) {
        super(database);
    }

    @Override
    protected void drop(ObjectIdentifier objectIdentifier, String fromSql, Transition transition) {
        ObjectIdentifier parent = objectIdentifier.getParent();
        String fqTableName = database.quote(".",
                parent.getCatalog(),
                parent.getSchema(),
                parent.getName()
        );
        String fqName = objectIdentifier.getName();
        String sql = "ALTER TABLE %s DROP CONSTRAINT %s".formatted(fqName, fqTableName);
        transition.addSql(sql);
    }
}
