package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.TransitionGenerator;

public class Transitions {
    protected final SqlServerDatabase database;
    final ForeignKeyTransitionGenerator foreignKeyTransitionGenerator;
    final TransitionGenerator changeCreateToAlterTransitionGenerator;
    final PrimaryKeyTransitionGenerator primaryKeyTransitionGenerator;
    final TransitionGenerator indexTransitionGenerator;
    final TableTransitionGenerator tableTransitionGenerator;

    public Transitions(SqlServerDatabase database) {
        this.database = database;
        changeCreateToAlterTransitionGenerator = new ChangeCreateToAlterTransitionGenerator(database);
        primaryKeyTransitionGenerator = new PrimaryKeyTransitionGenerator(database);
        indexTransitionGenerator = new IndexTransitionGenerator(database);
        foreignKeyTransitionGenerator = new ForeignKeyTransitionGenerator(database);
        tableTransitionGenerator = new TableTransitionGenerator(database);
    }
}
