package org.dandoy.dbpopd.code;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import static org.dandoy.dbpopd.utils.DbPopdFileUtils.toFile;

/**
 * DatabasechangeDetector runs every 3 seconds.
 * The first run will capture the signature of all the objects in the database.
 * The subsequent runs compare the database signatures with the reference.
 */
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
                        byte[] sqlHash = getHash(definition);
                        ObjectSignature oldSignature = targetObjectSignatures.get(objectIdentifier);
                        seen.remove(objectIdentifier);
                        if (oldSignature == null || !Arrays.equals(sqlHash, oldSignature.hash())) {
                            targetObjectSignatures.put(objectIdentifier, new ObjectSignature(modifyDate, sqlHash));
                            File file = DbPopdFileUtils.toFile(codeDirectory, objectIdentifier);
                            changeDetector.whenDatabaseChanged(
                                    file.exists() ? file : null,
                                    objectIdentifier,
                                    definition
                            );
                        }
                    }
                });

                for (ObjectIdentifier removedObjectIdentifier : seen) {
                    targetObjectSignatures.remove(removedObjectIdentifier);
                    File file = toFile(codeDirectory, removedObjectIdentifier);
                    if (file != null) {
                        changeDetector.whenDatabaseChanged(file, removedObjectIdentifier, null);
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
                MessageDigest messageDigest = ChangeDetector.getMessageDigest();
                for (String catalog : targetDatabase.getCatalogs()) {
                    databaseIntrospector.visitModuleDefinitions(catalog, new DatabaseVisitor() {
                        @Override
                        public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
                            byte[] hash = messageDigest.digest(definition.getBytes(StandardCharsets.UTF_8));
                            ret.put(objectIdentifier, new ObjectSignature(modifyDate, hash));
                            changeDetector.whenDatabaseChanged(toFile(configurationService.getCodeDirectory(), objectIdentifier), objectIdentifier, definition);
                        }
                    });
                }
                targetObjectSignatures = ret;
                hasScannedTargetCode = true;
            }
        }
    }

    byte[] getHash(String sql) {
        if (sql == null) return null;
        sql = ChangeDetector.cleanSql(sql);
        byte[] bytes = sql.getBytes(StandardCharsets.UTF_8);

        return ChangeDetector.getMessageDigest().digest(bytes);
    }

    byte[] getHash(ObjectIdentifier objectIdentifier) {
        if (objectIdentifier == null) return null;
        ObjectSignature objectSignature = targetObjectSignatures.get(objectIdentifier);
        if (objectSignature == null) return null;
        return objectSignature.hash;
    }

    public record ObjectSignature(Date modifyDate, byte[] hash) {}

    interface ChangeSession {

        void checkAllDatabaseObjects();

        void removeObjectIdentifier(ObjectIdentifier objectIdentifier);
    }
}
