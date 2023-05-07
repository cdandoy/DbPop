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

    @Test
    void name() {
        LocalCredentials localCredentials = LocalCredentials.from("mssql");
        try (Database targetDatabase = Database.createDatabase(localCredentials.targetConnectionBuilder())) {
            List<ObjectIdentifier> metaIdentifiers = new ArrayList<>();
            targetDatabase.createDatabaseIntrospector()
                    .visitModuleMetas(new DatabaseVisitor() {
                        @Override
                        public void moduleMeta(ObjectIdentifier objectIdentifier, Date modifyDate) {
                            if (!"INDEX".equals(objectIdentifier.getType())) {
                                metaIdentifiers.add(objectIdentifier);
                            }
                        }
                    }, "master");

            System.out.println("------------------------------------");
            List<ObjectIdentifier> defIdentifiers = new ArrayList<>();
            targetDatabase.createDatabaseIntrospector()
                    .visitModuleDefinitions(new DatabaseVisitor() {
                        @Override
                        public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String definition) {
                            if (!"INDEX".equals(objectIdentifier.getType())) {
                                defIdentifiers.add(objectIdentifier);
                            }
                        }
                    }, "master");

            System.out.println("------------------------------------");
            List<ObjectIdentifier> defIdentifiers2 = new ArrayList<>();
            targetDatabase.createDatabaseIntrospector()
                    .visitModuleDefinitions(new DatabaseVisitor() {
                        @Override
                        public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String definition) {
                            if (!"INDEX".equals(objectIdentifier.getType())) {
                                defIdentifiers2.add(objectIdentifier);
                            }
                        }
                    }, metaIdentifiers);

            Collections.sort(metaIdentifiers);
            Collections.sort(defIdentifiers);
            assertEquals(metaIdentifiers, defIdentifiers);

            Collections.sort(defIdentifiers2);
            assertEquals(defIdentifiers, defIdentifiers2);
        }
    }
}