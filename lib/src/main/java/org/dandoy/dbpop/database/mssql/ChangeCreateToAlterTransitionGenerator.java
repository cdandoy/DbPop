package org.dandoy.dbpop.database.mssql;

import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.Transition;
import org.dandoy.dbpop.database.TransitionGenerator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChangeCreateToAlterTransitionGenerator extends TransitionGenerator {
    private static final Pattern CREATE_PATTERN = Pattern.compile("(.*)\\bCREATE(\\s+(?:FUNCTION|PROC|PROCEDURE|TRIGGER|VIEW)\\b.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    public ChangeCreateToAlterTransitionGenerator(SqlServerDatabase database) {
        super(database);
    }

    @Override
    protected void before(ObjectIdentifier objectIdentifier, Transition transition) {
        transition.addSql("USE " + objectIdentifier.getCatalog());
    }

    @Override
    public String drop(ObjectIdentifier objectIdentifier) {
        return "DROP %s IF EXISTS %s".formatted(
                getKeyword(objectIdentifier.getType()),
                database.quote(".", objectIdentifier.getSchema(), objectIdentifier.getName())
        );
    }

    private static String getKeyword(String objectType) {
        return switch (objectType) {
            case "SQL_INLINE_TABLE_VALUED_FUNCTION",
                    "SQL_SCALAR_FUNCTION",
                    "SQL_TABLE_VALUED_FUNCTION" -> "FUNCTION";
            case "SQL_STORED_PROCEDURE" -> "PROCEDURE";
            case "SQL_TRIGGER" -> "TRIGGER";
            case "VIEW" -> "VIEW";
            default -> throw new RuntimeException("Unexpected object type: " + objectType);
        };
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
