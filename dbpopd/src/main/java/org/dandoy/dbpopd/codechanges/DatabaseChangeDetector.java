package org.dandoy.dbpopd.codechanges;

import ch.qos.logback.core.encoder.ByteArrayUtil;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;

import java.util.*;

import static org.dandoy.dbpopd.codechanges.CodeChangeService.debugObjectIdentifier;

@Slf4j
public class DatabaseChangeDetector {
    public record UpdatedSignatures(Map<ObjectIdentifier, ObjectSignature> signatures, Date lastModifiedDate) {}

    public static UpdatedSignatures getAllSignatures(Database database) {
        final Date[] lastModifiedDate = {new Date(0)};
        Map<ObjectIdentifier, ObjectSignature> objectSignatures = new HashMap<>();
        long timeDelta = HashCalculator.getTimeDelta(database);
        database.createDatabaseIntrospector().visitModuleDefinitions(new DatabaseVisitor() {
            @Override
            public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String sql) {
                ObjectSignature objectSignature = HashCalculator.getObjectSignature(modifyDate.getTime() - timeDelta, sql);
                if (objectIdentifier.equals(debugObjectIdentifier)) {
                    log.info("Database Signature {} | {} | [{}]",
                            objectIdentifier,
                            ByteArrayUtil.toHexString(objectSignature.hash()),
                            sql
                    );
                }
                objectSignatures.put(
                        objectIdentifier,
                        objectSignature
                );
                if (modifyDate.after(lastModifiedDate[0])) {
                    lastModifiedDate[0] = modifyDate;
                }
            }
        });
        return new UpdatedSignatures(objectSignatures, lastModifiedDate[0]);
    }

    public static UpdatedSignatures getUpdatedSignatures(Database database, @Nonnull Date modifiedSince, Map<ObjectIdentifier, ObjectSignature> oldSignatures) {
        Map<ObjectIdentifier, ObjectSignature> ret = new HashMap<>();
        List<ObjectIdentifier> objectIdentifiers = new ArrayList<>();
        final Date[] lastModifiedDate = {modifiedSince};

        // Visit all the definitions.
        // If the last modified date is newer than the last visit, add the identifier to objectIdentifiers
        // If it is older, copy the signature to the output
        database.createDatabaseIntrospector().visitModuleMetas(new DatabaseVisitor() {
            @Override
            public void moduleMeta(ObjectIdentifier objectIdentifier, Date modifyDate) {
                ObjectSignature oldSignature = oldSignatures.get(objectIdentifier);
                if (oldSignature == null) {
                    objectIdentifiers.add(objectIdentifier);    // We will need to fetch the SQL to get the new signature
                } else if (modifyDate.after(modifiedSince)) {
                    objectIdentifiers.add(objectIdentifier);    // We will need to fetch the SQL to get the new signature
                } else {
                    ret.put(objectIdentifier, oldSignature);    // Preserve the old signature
                }

                if (oldSignature != null && modifyDate.after(modifiedSince)) {
                    objectIdentifiers.add(objectIdentifier);
                } else {
                    ret.put(objectIdentifier, oldSignature);
                }
                if (modifyDate.after(lastModifiedDate[0])) {
                    lastModifiedDate[0] = modifyDate;
                }
            }
        });

        long timeDelta = HashCalculator.getTimeDelta(database);
        // Recalculate the signatures for the modified objects
        database.createDatabaseIntrospector().visitModuleDefinitions(objectIdentifiers, new DatabaseVisitor() {
            @Override
            public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String sql) {
                ObjectSignature objectSignature = HashCalculator.getObjectSignature(modifyDate.getTime() - timeDelta, sql);
                if (objectIdentifier.equals(debugObjectIdentifier)) {
                    log.info("Database Signature {} | {} | [{}]",
                            objectIdentifier,
                            ByteArrayUtil.toHexString(objectSignature.hash()),
                            sql
                    );
                }
                ret.put(objectIdentifier, objectSignature);
            }
        });
        return new UpdatedSignatures(ret, lastModifiedDate[0]);
    }
}
