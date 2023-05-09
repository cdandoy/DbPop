package org.dandoy.dbpop.database.mssql;

import org.dandoy.DbPopUtils;
import org.dandoy.LocalCredentials;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EnabledIf("org.dandoy.DbPopUtils#hasTargetMssql")
class SqlServerDatabaseIntrospectorTest {
    @BeforeEach
    void setUp() {
        DbPopUtils.prepareMssqlTarget();
    }

    /**
     * Verify that DatabaseIntrospector.visitModuleMetas() returns the same objects as the two versions of DatabaseIntrospector.visitModuleDefinitions()
     */
    @Test
    void name() {
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Database targetDatabase = Database.createDatabase(localCredentials.targetConnectionBuilder())) {
            List<ObjectIdentifier> metaIdentifiers = new ArrayList<>();
            targetDatabase.createDatabaseIntrospector()
                    .visitModuleMetas(new DatabaseVisitor() {
                        @Override
                        public void moduleMeta(ObjectIdentifier objectIdentifier, Date modifyDate) {
                            metaIdentifiers.add(objectIdentifier);
                        }
                    }, "master");

            List<ObjectIdentifier> defIdentifiers = new ArrayList<>();
            targetDatabase.createDatabaseIntrospector()
                    .visitModuleDefinitions("master", new DatabaseVisitor() {
                        @Override
                        public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String definition) {
                            defIdentifiers.add(objectIdentifier);
                        }
                    });

            List<ObjectIdentifier> defIdentifiers2 = new ArrayList<>();
            targetDatabase.createDatabaseIntrospector()
                    .visitModuleDefinitions(metaIdentifiers, new DatabaseVisitor() {
                        @Override
                        public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String definition) {
                            defIdentifiers2.add(objectIdentifier);
                        }
                    });

            Collections.sort(metaIdentifiers);
            Collections.sort(defIdentifiers);
            assertEquals(metaIdentifiers, defIdentifiers);

            Collections.sort(defIdentifiers2);
            assertEquals(defIdentifiers, defIdentifiers2);
        }
    }
}