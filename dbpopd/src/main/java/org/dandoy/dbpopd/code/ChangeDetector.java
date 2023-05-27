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
import org.dandoy.dbpopd.site.SiteWebSocket;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;
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
    private final List<Change> changes = new ArrayList<>();

    public ChangeDetector(ConfigurationService configurationService, SiteWebSocket siteWebSocket) {
        this.configurationService = configurationService;
        this.siteWebSocket = siteWebSocket;
        this.databaseChangeDetector = new DatabaseChangeDetector(configurationService, this);
        this.fileChangeDetector = new FileChangeDetector(configurationService, this);
    }

    public List<Change> getChanges() {
        return new ArrayList<>(changes);
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
        holdingChanges(changeSession -> {
            databaseChangeDetector.checkCodeChanges();
        });
    }

    static String cleanSql(String sql) {
        sql = sql
                .replace("\r\n", "\n")  // Windows
                .replace("\r", "\n");   // Mac

        // trim empty lines at the end of the text
        while (sql.endsWith("\n")) {
            sql = sql.substring(0, sql.length() - 1);
            sql = sql.trim();
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

    private boolean areSameHash(File file, ObjectIdentifier objectIdentifier) {
        byte[] fileHash = fileChangeDetector.getHash(file);
        byte[] dbHash = databaseChangeDetector.getHash(objectIdentifier);
        return Arrays.equals(fileHash, dbHash);
    }

    private void sendCodeChangeMessage() {
        siteWebSocket.codeChanged();
    }

    void setHasCode(boolean hasCode) {
        siteWebSocket.setHasCode(hasCode);
    }

    private ChangeFile getChangeFile() {
        return new ChangeFile(new File(configurationService.getCodeDirectory(), "changes.txt"));
    }

    Change removeChange(ObjectIdentifier objectIdentifier) {
        for (int i = 0; i < changes.size(); i++) {
            Change change = changes.get(i);
            if (objectIdentifier.equals(change.getObjectIdentifier())) {
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
        return siteWebSocket.holdChanges(() -> {
            AtomicBoolean hasChanged = new AtomicBoolean();
            Set<ObjectIdentifier> removeIdentifiers = new HashSet<>();
            AtomicBoolean checkAllDatabase = new AtomicBoolean(false);
            AtomicBoolean checkAllFiles = new AtomicBoolean(false);
            try {
                return function.apply(new ChangeSession() {
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
                    public void checkAllFiles() {
                        checkAllFiles.set(true);
                    }
                });
            } finally {
                if (checkAllDatabase.get()) {
                    databaseChangeDetector.captureObjectSignatures();
                }
                if (checkAllFiles.get()) {
                    fileChangeDetector.checkAllFiles();
                }
                for (ObjectIdentifier objectIdentifier : removeIdentifiers) {
                    removeChange(objectIdentifier);
                }
                if (hasChanged.get()) {
                    sendCodeChangeMessage();
                }
            }
        });
    }

    synchronized void holdingChanges(Consumer<ChangeSession> consumer) {
        holdingChanges(changeSession -> {
            consumer.accept(changeSession);
            return null;
        });
    }

    interface ChangeSession {
        void checkAllDatabaseObjects();

        void removeObjectIdentifier(ObjectIdentifier objectIdentifier);

        void checkAllFiles();
    }

    synchronized void whenDatabaseObjectChanged(@NotNull ObjectIdentifier objectIdentifier, @Nullable String sql) {
        File file = DbPopdFileUtils.toFile(configurationService.getCodeDirectory(), objectIdentifier);

        if (applyChanges) {
            try (ChangeFile changeFile = getChangeFile()) {
                log.info("Downloading {}", file);
                writeDefinition(file, sql);
                changeFile.objectUpdated(objectIdentifier);
            }
        } else {
            Change change = removeChange(objectIdentifier);
            if (!areSameHash(file, sql)) {
                if (change == null) {
                    change = new Change(file, sql == null ? null : objectIdentifier);
                }
                change.setDatabaseChanged();
                changes.add(change);
            }
            sendCodeChangeMessage();
        }
    }

    synchronized void whenDatabaseObjectDeleted(@NotNull ObjectIdentifier objectIdentifier) {
        File file = DbPopdFileUtils.toFile(configurationService.getCodeDirectory(), objectIdentifier);
        if (applyChanges) {
            try (ChangeFile changeFile = getChangeFile()) {
                log.info("deleting {}", file);
                changeFile.objectDeleted(objectIdentifier);
                if (!file.delete() && file.exists()) {
                    log.error("Failed to delete " + file);
                }
            }
        } else {
            Change change = removeChange(objectIdentifier);
            if (file.isFile()) {
                if (change == null) {
                    change = new Change(file, objectIdentifier);
                }
                change.setDatabaseDeleted();
                changes.add(change);
            }
            sendCodeChangeMessage();
        }
    }

    synchronized void whenFileChanged(@NotNull File file) {
        ObjectIdentifier objectIdentifier = DbPopdFileUtils.toObjectIdentifier(configurationService.getCodeDirectory(), file);
        if (objectIdentifier == null) return; // Not a DB file
        if (applyChanges) {
            try (ChangeFile changeFile = getChangeFile()) {
                if (!areSameHash(file, objectIdentifier)) {
                    try (Database targetDatabase = configurationService.getTargetDatabaseCache()) {
                        log.info("Executing {}", file);
                        String sql = IOUtils.toString(file);
                        Connection connection = targetDatabase.getConnection();
                        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                            preparedStatement.execute();
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                changeFile.objectUpdated(objectIdentifier);
            }
        } else {
            Change change = removeChange(objectIdentifier);
            if (!areSameHash(file, objectIdentifier)) {
                if (change == null) {
                    change = new Change(file, objectIdentifier);
                }
                change.setFileChanged();
                changes.add(change);
            }
            sendCodeChangeMessage();
        }
    }

    synchronized void whenFileDeleted(@NotNull File file) {
        ObjectIdentifier objectIdentifier = DbPopdFileUtils.toObjectIdentifier(configurationService.getCodeDirectory(), file);
        if (objectIdentifier == null) return; // Not a SQL file
        if (applyChanges) {
            try (ChangeFile changeFile = getChangeFile()) {
                try (Database targetDatabase = configurationService.getTargetDatabaseCache()) {
                    log.info("Deleting {}", objectIdentifier);
                    targetDatabase.dropObject(objectIdentifier);
                    changeFile.objectDeleted(objectIdentifier);
                }
            }
        } else {
            Change change = removeChange(objectIdentifier);
            if (!areSameHash(file, objectIdentifier)) {
                if (change == null) {
                    change = new Change(file, objectIdentifier);
                }
                change.setFileDeleted();
                changes.add(change);
            }
            sendCodeChangeMessage();
        }
    }
}
