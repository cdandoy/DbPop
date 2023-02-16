package org.dandoy.dbpopd.code;

import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.ConfigurationService;

import java.io.File;
import java.sql.*;
import java.util.Date;

@Singleton
@Slf4j
public class ChangeDetector {
    private final ConfigurationService configurationService;

    public ChangeDetector(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @Scheduled(fixedDelay = "3s", initialDelay = "3s")
    void checkCodeChanges() {
        if (configurationService.isCodeAutoSave()) {
            try (Database targetDatabase = configurationService.createTargetDatabase()) {
                Timestamp waterMark = getWaterMark(targetDatabase);
                if (waterMark != null) {
                    DatabaseIntrospector databaseIntrospector = targetDatabase.createDatabaseIntrospector();
                    File codeDirectory = configurationService.getCodeDirectory();
                    try (CodeDB.TimestampInserter timestampInserter = CodeDB.createTimestampInserter(targetDatabase)) {
                        try (TargetDbToFileVisitor dbToFileVisitor = new MyTargetDbToFileVisitor(timestampInserter, databaseIntrospector, codeDirectory)) {
                            databaseIntrospector.visitModuleDefinitions(dbToFileVisitor, waterMark);
                        }
                    }
                }
            } catch (SQLException e) {
                log.error("Failed to check for code changes", e);
            }
        }
    }

    private Timestamp getWaterMark(Database targetDatabase) throws SQLException {
        try {
            Connection targetConnection = targetDatabase.getConnection();
            try (PreparedStatement preparedStatement = targetConnection.prepareStatement("SELECT MAX(code_timestamp) FROM master.dbo.dbpop_timestamps")) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        return resultSet.getTimestamp(1);
                    } else {
                        return null;
                    }
                }
            }
        } catch (SQLException e) {
            if ("S0002".equals(e.getSQLState())) {
                return null;
            }
            throw e;
        }
    }

    @Slf4j
    private static class MyTargetDbToFileVisitor extends TargetDbToFileVisitor {
        public MyTargetDbToFileVisitor(CodeDB.TimestampInserter timestampInserter, DatabaseIntrospector databaseIntrospector, File codeDirectory) {super(timestampInserter, databaseIntrospector, codeDirectory);}

        @Override
        public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
            log.info("Saving {}", objectIdentifier);
            super.moduleDefinition(objectIdentifier, modifyDate, definition);
        }
    }
}
