package org.dandoy.dbpopd.code;

import io.micronaut.context.annotation.Context;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
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
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

@Singleton
@Slf4j
@Context
public class ChangeDetector {
    private final ConfigurationService configurationService;
    private final SiteWebSocket siteWebSocket;
    @Getter
    private final DatabaseChangeDetector databaseChangeDetector;
    @Getter
    private final FileChangeDetector fileChangeDetector;
    @Setter
    private boolean applyChanges = false;
    @Getter
    private final List<Change> changes = new ArrayList<>();

    public ChangeDetector(ConfigurationService configurationService, SiteWebSocket siteWebSocket) {
        this.configurationService = configurationService;
        this.siteWebSocket = siteWebSocket;
        this.databaseChangeDetector = new DatabaseChangeDetector(configurationService, this);
        this.fileChangeDetector = new FileChangeDetector(configurationService, this);
    }

    @PostConstruct
    void postContruct() {
        fileChangeDetector.postContruct();
    }

    @PreDestroy
    void preDestroy() {
        fileChangeDetector.preDestroy();
    }

    @Scheduled(fixedDelay = "3s", initialDelay = "3s")
    void checkDatabaseCodeChanges() {
        databaseChangeDetector.checkCodeChanges();
    }

    static String cleanSql(String sql) {
        // \r\n => \n
        sql = sql.replace("\r", "");

        // trim \n at the end of the text
        while (sql.endsWith("\n")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql;
    }

    @SneakyThrows
    static MessageDigest getMessageDigest() {
        return MessageDigest.getInstance("SHA-256");
    }

    private boolean areSameHash(File file, String sql) {
        byte[] fileHash = fileChangeDetector.getHash(file);
        byte[] dbHash = databaseChangeDetector.getHash(sql);
        return Arrays.equals(fileHash, dbHash);
    }

    synchronized void whenFileChanged(@Nullable File file, @NotNull ObjectIdentifier objectIdentifier) {
        if (applyChanges) {
            try (ChangeFile changeFile = getChangeFile()) {
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
            // We assume that whenever the file is changed, it must be applied to the database
            if (change == null) {
                change = new Change(file, objectIdentifier);
            }
            change.setFileChanged(true);
            changes.add(change);
            sendCodeChangeMessage();
        }
    }

    private void sendCodeChangeMessage() {
        siteWebSocket.sendMessage(CodeChangeMessage.MESSAGE);
    }

    synchronized void whenDatabaseChanged(File file, @NotNull ObjectIdentifier objectIdentifier, @Nullable String definition) {
        if (applyChanges) {
            try (ChangeFile changeFile = getChangeFile()) {
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
            if (!areSameHash(file, definition)) {
                if (change == null) {
                    change = new Change(file, definition == null ? null : objectIdentifier);
                }
                change.setDatabaseChanged(true);
                changes.add(change);
                sendCodeChangeMessage();
            }
        }
    }

    private ChangeFile getChangeFile() {
        return new ChangeFile(new File(configurationService.getCodeDirectory(), "changes.txt"));
    }

    private Change removeChange(File file, ObjectIdentifier objectIdentifier) {
        for (int i = 0; i < changes.size(); i++) {
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

    /**
     * Safely executes the function without checking for code changes
     */
    synchronized <R> R holdingChanges(Function<ChangeSession, R> function) {
        AtomicBoolean hasChanged = new AtomicBoolean();
        Set<File> checkFiles = new HashSet<>();
        Set<ObjectIdentifier> checkIdentifiers = new HashSet<>();
        Set<File> removeFiles = new HashSet<>();
        Set<ObjectIdentifier> removeIdentifiers = new HashSet<>();
        AtomicBoolean checkAllFiles = new AtomicBoolean(false);
        AtomicBoolean checkAllDatabase = new AtomicBoolean(false);
        try {
            return function.apply(new ChangeSession() {
                @Override
                public void check(ObjectIdentifier objectIdentifier) {
                    checkIdentifiers.add(objectIdentifier);
                }

                @Override
                public void checkAllDatabaseObjects() {
                    checkAllDatabase.set(true);
                }

                @Override
                public void removeObjectIdentifier(ObjectIdentifier objectIdentifier) {
                    removeIdentifiers.add(objectIdentifier);
                    hasChanged.set(true);
                }

                @Override
                public void check(File file) {
                    checkFiles.add(file);
                }

                @Override
                public void removeFile(File file) {
                    removeFiles.add(file);
                    hasChanged.set(true);
                }

                @Override
                public void checkAllFiles() {
                    checkAllFiles.set(true);
                }
            });
        } finally {
            if (checkAllDatabase.get()) {
                databaseChangeDetector.captureObjectSignatures();
            } else {
                for (ObjectIdentifier objectIdentifier : removeIdentifiers) {
                    removeChange(null, objectIdentifier);
                }
                for (ObjectIdentifier objectIdentifier : checkIdentifiers) {
                    //FIXME: databaseChangeDetector.captureObjectSignature(objectIdentifier);
                }
            }
            if (checkAllFiles.get()) {
                // FIXME: fileChangeDetector.captureAllSignatures();
            } else {
                for (File file : removeFiles) {
                    removeChange(file, null);
                }
                for (File file : checkFiles) {
                    // FIXME: fileChangeDetector.captureSignature(file);
                }
            }
            if (hasChanged.get()) {
                sendCodeChangeMessage();
            }
        }
    }

    synchronized void holdingChanges(Consumer<ChangeSession> consumer) {
        holdingChanges(changeSession -> {
            consumer.accept(changeSession);
            return null;
        });
    }

    interface ChangeSession extends DatabaseChangeDetector.ChangeSession, FileChangeDetector.ChangeSession {
    }
}
