package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.Transition;
import org.dandoy.dbpop.database.TransitionGenerator;

public class ForeignKeyTransitionGenerator extends TransitionGenerator {
    public ForeignKeyTransitionGenerator(SqlServerDatabase database) {
        super(database);
    }

    @Override
    protected void drop(ObjectIdentifier objectIdentifier, String fromSql, Transition transition) {
        ObjectIdentifier parent = objectIdentifier.getParent();
        String fqTableName = database.quote(parent);
        String fqName = objectIdentifier.getName();
        String sql = "ALTER TABLE %s DROP CONSTRAINT %s".formatted(fqTableName, database.quote(fqName));
        transition.addSql(sql);
    }
}
