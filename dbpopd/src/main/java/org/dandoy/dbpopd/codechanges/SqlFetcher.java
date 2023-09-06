package org.dandoy.dbpopd.codechanges;

import jakarta.annotation.Nullable;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class to fetch the SQL definition from the database.
 */
public class SqlFetcher {
    private final Map<ObjectIdentifier, String> result = new HashMap<>();

    static String fetchSql(Database database, ObjectIdentifier objectIdentifier) {
        return fetchSql(database, List.of(objectIdentifier)).getSql(objectIdentifier);
    }

    static SqlFetcher fetchSql(Database database, List<ObjectIdentifier> objectIdentifiers) {
        return new SqlFetcher().doit(database, objectIdentifiers);
    }

    private SqlFetcher doit(Database database, List<ObjectIdentifier> objectIdentifiers) {
        database.createDatabaseIntrospector().visitModuleDefinitions(objectIdentifiers, new DatabaseVisitor() {
            @Override
            public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String sql) {
                result.put(objectIdentifier, sql);
            }
        });
        return this;
    }

    @Nullable
    String getSql(ObjectIdentifier objectIdentifier) {
        return result.get(objectIdentifier);
    }
}
