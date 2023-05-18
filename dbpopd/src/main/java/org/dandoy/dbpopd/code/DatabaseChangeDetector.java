package org.dandoy.dbpopd.code;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.ConfigurationService;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

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
                    public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String sql) {
                        byte[] sqlHash = getHash(sql);
                        ObjectSignature oldSignature = targetObjectSignatures.get(objectIdentifier);
                        seen.remove(objectIdentifier);
                        if (oldSignature == null || !Arrays.equals(sqlHash, oldSignature.hash())) {
                            targetObjectSignatures.put(objectIdentifier, new ObjectSignature(modifyDate, sqlHash));
                            changeDetector.whenDatabaseObjectChanged(objectIdentifier, sql);
                        }
                    }
                });

                for (ObjectIdentifier removedObjectIdentifier : seen) {
                    targetObjectSignatures.remove(removedObjectIdentifier);
                    changeDetector.whenDatabaseObjectDeleted(removedObjectIdentifier);
                }
            }
        }
    }

    synchronized void captureObjectSignatures() {
        changeDetector.holdingChanges(changeSession -> {
            try (Database targetDatabase = safeGetTargetDatabase()) {
                if (targetDatabase != null) {
                    Map<ObjectIdentifier, ObjectSignature> ret = new HashMap<>();
                    DatabaseIntrospector databaseIntrospector = targetDatabase.createDatabaseIntrospector();
                    MessageDigest messageDigest = ChangeDetector.getMessageDigest();
                    for (String catalog : targetDatabase.getCatalogs()) {
                        databaseIntrospector.visitModuleDefinitions(catalog, new DatabaseVisitor() {
                            @Override
                            public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String sql) {
                                byte[] hash = messageDigest.digest(sql.getBytes(StandardCharsets.UTF_8));
                                ret.put(objectIdentifier, new ObjectSignature(modifyDate, hash));
                                changeDetector.whenDatabaseObjectChanged(objectIdentifier, sql);
                            }
                        });
                    }
                    targetObjectSignatures = ret;
                    hasScannedTargetCode = true;
                }
            }
        });
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
}
