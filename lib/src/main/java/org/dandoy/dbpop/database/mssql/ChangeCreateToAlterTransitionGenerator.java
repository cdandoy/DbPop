package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.Transition;
import org.dandoy.dbpop.database.TransitionGenerator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ChangeCreateToAlterTransitionGenerator extends TransitionGenerator {
    private static final Pattern CREATE_PATTERN = Pattern.compile("(.*)\\bCREATE(\\s+(?:FUNCTION|PROC|PROCEDURE|TRIGGER|VIEW)\\b.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    public ChangeCreateToAlterTransitionGenerator(Database database) {
        super(database);
    }

    @Override
    protected void before(ObjectIdentifier objectIdentifier, Transition transition) {
        transition.addSql("USE " + objectIdentifier.getCatalog());
    }

    @Override
    protected void drop(ObjectIdentifier objectIdentifier, String fromSql, Transition transition) {
        transition.addSql("DROP PROCEDURE IF EXISTS " + database.quote(".", objectIdentifier.getSchema(), objectIdentifier.getName()));
    }

    @Override
    protected void update(ObjectIdentifier objectIdentifier, String fromSql, String toSql, Transition transition) {
        Matcher matcher = CREATE_PATTERN.matcher(toSql);
        if (matcher.matches()) {
            String pre = matcher.group(1);
            String post = matcher.group(2);
            transition.addSql(pre + "ALTER" + post);
        } else {
            transition.setError("Failed to generate the ALTER version for %s", objectIdentifier.toQualifiedName());
        }
    }
}
