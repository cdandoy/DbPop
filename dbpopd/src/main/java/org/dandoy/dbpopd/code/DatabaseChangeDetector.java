package org.dandoy.dbpopd.code;

import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;
import org.dandoy.dbpopd.utils.IOUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.Supplier;

import static org.dandoy.dbpopd.utils.DbPopdFileUtils.toFile;

/**
 * DatabasechangeDetector runs every 3 seconds.
 * The first run will capture the signature of all the objects in the database.
 * The subsequent runs compare the database signatures with the reference.
 */
@Singleton
@Slf4j
public class DatabaseChangeDetector {
    private final ConfigurationService configurationService;
    private final ChangeDetector changeDetector;
    private boolean hasScannedTargetCode = false;
    private Map<ObjectIdentifier, ObjectSignature> targetObjectSignatures = new HashMap<>();

    public DatabaseChangeDetector(ConfigurationService configurationService, ChangeDetector changeDetector) {
        this.configurationService = configurationService;
        this.changeDetector = changeDetector;
    }

    @Scheduled(fixedDelay = "3s", initialDelay = "3s")
    void checkCodeChanges() {
        if (configurationService.isCodeAutoSave()) {
            long t0 = System.currentTimeMillis();
            if (!hasScannedTargetCode) {
                captureObjectSignatures();
            } else {
                compareObjectSignatures();
            }
            long t1 = System.currentTimeMillis();
            log.trace("checkCodeChanges - {}ms", t1 - t0);
        }
    }

    private Database safeGetTargetDatabase() {
        try {
            return configurationService.createTargetDatabase();
        } catch (Exception e) {
            return null;
        }
    }

    synchronized void compareObjectSignatures() {
        try (Database targetDatabase = safeGetTargetDatabase()) {
            if (targetDatabase != null) {
                Set<ObjectIdentifier> seen = new HashSet<>(targetObjectSignatures.keySet());

                File codeDirectory = configurationService.getCodeDirectory();
                // The file already contains that code
                DatabaseIntrospector databaseIntrospector = targetDatabase.createDatabaseIntrospector();
                MessageDigest messageDigest = getMessageDigest();
                List<ObjectIdentifier> changedIdentifiers = new ArrayList<>();
                for (String catalog : targetDatabase.getCatalogs()) {
                    databaseIntrospector.visitModuleMetas(catalog, new DatabaseVisitor() {
                        @Override
                        public void moduleMeta(ObjectIdentifier objectIdentifier, Date modifyDate) {
                            ObjectSignature oldSignature = targetObjectSignatures.get(objectIdentifier);
                            seen.remove(objectIdentifier);
                            if (oldSignature == null || (modifyDate != null && !modifyDate.equals(oldSignature.modifyDate))) {
                                changedIdentifiers.add(objectIdentifier);
                            }
                        }
                    });
                }

                databaseIntrospector.visitModuleDefinitions(changedIdentifiers, new DatabaseVisitor() {
                    @Override
                    public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
                        byte[] sqlHash = messageDigest.digest(definition.getBytes(StandardCharsets.UTF_8));
                        ObjectSignature oldSignature = targetObjectSignatures.get(objectIdentifier);
                        seen.remove(objectIdentifier);
                        if (oldSignature == null || !Arrays.equals(sqlHash, oldSignature.hash())) {
                            targetObjectSignatures.put(objectIdentifier, new ObjectSignature(modifyDate, sqlHash));
                            File file = DbPopdFileUtils.toFile(codeDirectory, objectIdentifier);
                            boolean fileExists = file.exists();
                            if (fileExists) {
                                byte[] fileHash = getFileHash(file);
                                if (!Arrays.equals(fileHash, sqlHash)) { // Don't signal a change if that's what we already have on file
                                    changeDetector.whenDatabaseChanged(file, objectIdentifier, definition);
                                }
                            } else {
                                changeDetector.  whenDatabaseChanged(null, objectIdentifier, definition);
                            }
                        }
                    }
                });

                for (ObjectIdentifier removedObjectIdentifier : seen) {
                    targetObjectSignatures.remove(removedObjectIdentifier);
                    File file = toFile(codeDirectory, removedObjectIdentifier);
                    if (file != null) {
                        changeDetector.  whenDatabaseChanged(file, removedObjectIdentifier, null);
                    }
                }
            }
        }
    }
    synchronized void captureObjectSignatures() {
        try (Database targetDatabase = safeGetTargetDatabase()) {
            if (targetDatabase != null) {
                Map<ObjectIdentifier, ObjectSignature> ret = new HashMap<>();
                DatabaseIntrospector databaseIntrospector = targetDatabase.createDatabaseIntrospector();
                MessageDigest messageDigest = getMessageDigest();
                for (String catalog : targetDatabase.getCatalogs()) {
                    databaseIntrospector.visitModuleDefinitions(catalog, new DatabaseVisitor() {
                        @Override
                        public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
                            byte[] hash = messageDigest.digest(definition.getBytes(StandardCharsets.UTF_8));
                            ret.put(objectIdentifier, new ObjectSignature(modifyDate, hash));
                        }
                    });
                }
                targetObjectSignatures = ret;
                hasScannedTargetCode = true;
            }
        }
    }

    /**
     * Safely executes the supplier without checking for code changes
     */
    synchronized <T> T holdingChanges(Supplier<T> supplier) {
        try {
            return supplier.get();
        } finally {
            captureObjectSignatures();
        }
    }
    @SneakyThrows
    private static MessageDigest getMessageDigest() {
        return MessageDigest.getInstance("SHA-256");
    }

    @SuppressWarnings("unused")
    @SneakyThrows
    public Map<ObjectIdentifier, ObjectSignature> getObjectDefinitions(File directory) {
        Map<ObjectIdentifier, ObjectSignature> ret = new HashMap<>();
        Path directoryPath = directory.toPath();
        int directoryPathNameCount = directoryPath.getNameCount();
        Files.walkFileTree(directoryPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                if (path.getNameCount() == directoryPathNameCount + 4) {
                    String catalog = path.getName(directoryPathNameCount).toString();
                    String schema = path.getName(directoryPathNameCount + 1).toString();
                    String objectType = path.getName(directoryPathNameCount + 2).toString();
                    String filename = path.getName(directoryPathNameCount + 3).toString();
                    if (filename.endsWith(".sql")) {
                        String objectName = filename.substring(0, filename.length() - 4);
                        File file = path.toFile();
                        byte[] hash = getFileHash(file);
                        ret.put(
                                new ObjectIdentifier(
                                        objectType,
                                        catalog,
                                        schema,
                                        objectName
                                ),
                                new ObjectSignature(
                                        new Date(file.lastModified()),
                                        hash
                                )
                        );
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return ret;
    }

    @SneakyThrows
    public byte[] getFileHash(File file) {
        MessageDigest messageDigest = getMessageDigest();
        byte[] bytes = IOUtils.toBytes(file);
        return messageDigest.digest(bytes);
    }

    public record ObjectSignature(Date modifyDate, byte[] hash) {}

}
