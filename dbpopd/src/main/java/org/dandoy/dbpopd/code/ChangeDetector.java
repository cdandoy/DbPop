package org.dandoy.dbpopd.code;

import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.site.CodeChangeMessage;
import org.dandoy.dbpopd.site.SiteWebSocket;
import org.dandoy.dbpopd.utils.IOUtils;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Slf4j
public class ChangeDetector {
    private final ConfigurationService configurationService;
    private final SiteWebSocket siteWebSocket;
    @Setter
    private boolean applyChanges = false;
    @Getter
    private final List<Change> changes = new ArrayList<>();

    public ChangeDetector(ConfigurationService configurationService, SiteWebSocket siteWebSocket) {
        this.configurationService = configurationService;
        this.siteWebSocket = siteWebSocket;
    }

    synchronized void whenFileChanged(@Nullable File file, @NotNull ObjectIdentifier objectIdentifier) {
        if (applyChanges) {
            try (ChangeFile changeFile = new ChangeFile(new File(configurationService.getCodeDirectory(), "changes.txt"))) {
                try (Database targetDatabase = configurationService.getTargetDatabaseCache()) {
                    if (file != null) {
                        log.info("Executing {}", file);
                        String sql = IOUtils.toString(file);
                        Connection connection = targetDatabase.getConnection();
                        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                            preparedStatement.execute();
                        }
                        changeFile.objectUpdated(objectIdentifier);
                    } else {
                        log.info("Deleting {}", objectIdentifier);
                        targetDatabase.dropObject(objectIdentifier);
                        changeFile.objectDeleted(objectIdentifier);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            Change change = removeChange(file, objectIdentifier);
            if (change == null) {
                change = new Change(file, objectIdentifier);
            }
            change.setFileChanged(true);
            changes.add(change);
            siteWebSocket.sendMessage(CodeChangeMessage.MESSAGE);
        }
    }

    synchronized void whenDatabaseChanged(File file, @NotNull ObjectIdentifier objectIdentifier, @Nullable String definition) {
        if (applyChanges) {
            try (ChangeFile changeFile = new ChangeFile(new File(configurationService.getCodeDirectory(), "changes.txt"))) {
                if (definition != null) {
                    log.info("Downloading {}", file);
                    writeDefinition(file, definition);
                    changeFile.objectUpdated(objectIdentifier);
                } else {
                    log.info("deleting {}", file);
                    changeFile.objectDeleted(objectIdentifier);
                    if (!file.delete() && file.exists()) {
                        log.error("Failed to delete " + file);
                    }
                }
            }
        } else {
            Change change = removeChange(file, objectIdentifier);
            if (change == null) {
                change = new Change(file, objectIdentifier);
            }
            change.setDatabaseChanged(true);
            changes.add(change);
        }
        siteWebSocket.sendMessage(CodeChangeMessage.MESSAGE);
    }

    private Change removeChange(File file, ObjectIdentifier objectIdentifier) {
        for (int i = changes.size() - 1; i >= 0; i--) {
            Change change = changes.get(i);
            if (change.equals(file, objectIdentifier)) {
                changes.remove(i);
                return change;
            }
        }
        return null;
    }

    private void writeDefinition(File file, String definition) {
        File directory = file.getParentFile();
        if (!directory.mkdirs() && !directory.isDirectory()) {
            log.error("Failed to create the directory " + directory, new Exception());
        } else {
            try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(definition.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                log.error("Failed to write to " + file, e);
            }
        }
    }

}
