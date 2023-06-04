package org.dandoy.dbpop.database;

import static org.dandoy.dbpop.utils.StringUtils.normalizeEOL;

public abstract class TransitionGenerator {
    protected final Database database;

    protected TransitionGenerator(Database database) {this.database = database;}

    public Transition generateTransition(ObjectIdentifier objectIdentifier, String fromSql, String toSql) {
        Transition transition = new Transition(objectIdentifier);
        generateTransition(
                objectIdentifier,
                normalizeEOL(fromSql),
                normalizeEOL(toSql),
                transition
        );
        return transition;
    }

    protected void generateTransition(ObjectIdentifier objectIdentifier, String fromSql, String toSql, Transition transition) {
        before(objectIdentifier, transition);
        if (fromSql == null) {
            create(objectIdentifier, toSql, transition);
        } else if (toSql == null) {
            drop(objectIdentifier, fromSql, transition);
        } else {
            update(objectIdentifier, fromSql, toSql, transition);
        }
        after(objectIdentifier, transition);
    }

    protected void before(ObjectIdentifier objectIdentifier, Transition transition) {
    }

    protected void after(ObjectIdentifier objectIdentifier, Transition transition) {
    }

    protected void create(ObjectIdentifier objectIdentifier, String toSql, Transition transition) {
        transition.addSql(toSql);
    }

    protected void drop(ObjectIdentifier objectIdentifier, String fromSql, Transition transition) {
        throw new RuntimeException("Missing drop implementation in " + getClass().getSimpleName());
    }

    protected void update(ObjectIdentifier objectIdentifier, String fromSql, String toSql, Transition transition) {
        drop(objectIdentifier, fromSql, transition);
        create(objectIdentifier, toSql, transition);
    }
}
