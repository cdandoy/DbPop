package org.dandoy.dbpopd.mssql;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.tests.TestUtils;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.code.DbToFileVisitor;
import org.dandoy.dbpopd.junit.DbPopTest;
import org.junit.jupiter.api.Test;

import java.io.File;

@DbPopTest
@MicronautTest(environments = "temp-test")
class DbToFileVisitorTest {
    @Inject
    ConfigurationService configurationService;

    @Test
    void name() {
        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            DatabaseIntrospector databaseIntrospector = sourceDatabase.createDatabaseIntrospector();
            try (DbToFileVisitor visitor = new DbToFileVisitor(databaseIntrospector, configurationService.getCodeDirectory())) {
                databaseIntrospector.visit(visitor);
            }
        }
    }
}