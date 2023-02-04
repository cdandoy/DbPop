package org.dandoy.dbpopd.code;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.tests.TestUtils;
import org.dandoy.dbpopd.ConfigurationService;
import org.junit.jupiter.api.Test;

import java.io.File;

@MicronautTest
class DbToFileVisitorTest {
    @Inject
    ConfigurationService configurationService;

    @Test
    void name() {
        try (Database database = configurationService.createSourceDatabase()) {
            DatabaseIntrospector databaseIntrospector = database.createDatabaseIntrospector();
            try (DbToFileVisitor visitor = new DbToFileVisitor(databaseIntrospector, configurationService.getCodeDirectory())) {
                databaseIntrospector.visit(visitor);
            }
        } finally {
            TestUtils.delete(new File(TestUtils.SRC_DIR, "code"));
        }
    }
}