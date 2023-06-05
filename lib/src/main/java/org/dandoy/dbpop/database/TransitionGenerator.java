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
            transition.addSql(toSql);
        } else if (toSql == null) {
            transition.addSql(drop(objectIdentifier));
        } else {
            update(objectIdentifier, fromSql, toSql, transition);
        }
    }

    protected void before(ObjectIdentifier objectIdentifier, Transition transition) {
    }

    public String drop(ObjectIdentifier objectIdentifier) {
        throw new RuntimeException("Missing drop implementation in " + getClass().getSimpleName());
    }

    protected void update(ObjectIdentifier objectIdentifier, String fromSql, String toSql, Transition transition) {
        transition.addSql(drop(objectIdentifier));
        transition.addSql(toSql);
    }
}
