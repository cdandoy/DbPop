package org.dandoy.dbpopd.code2;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;

import java.util.*;

@Slf4j
public class DatabaseChangeDetector {
   public record UpdatedSignatures(Map<ObjectIdentifier, ObjectSignature> signatures, Date lastModifiedDate) {}

    public static UpdatedSignatures getAllSignatures(Database database) {
        final Date[] lastModifiedDate = {new Date(0)};
        Map<ObjectIdentifier, ObjectSignature> objectSignatures = new HashMap<>();
        database.createDatabaseIntrospector().visitModuleDefinitions(new DatabaseVisitor() {
            @Override
            public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String definition) {
                objectSignatures.put(
                        objectIdentifier,
                        HashCalculator.getObjectSignature(objectIdentifier.getType(), definition)
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
                if (modifyDate.after(modifiedSince)) { // We will need to fetch the SQL to get the new signature
                    objectIdentifiers.add(objectIdentifier);
                } else { // Preserve the old signature
                    ret.put(objectIdentifier, oldSignatures.get(objectIdentifier));
                }
                if (modifyDate.after(lastModifiedDate[0])) {
                    lastModifiedDate[0] = modifyDate;
                }
            }
        });

        // Recalculate the signatures for the modified objects
        database.createDatabaseIntrospector().visitModuleDefinitions(objectIdentifiers, new DatabaseVisitor() {
            @Override
            public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String definition) {
                ObjectSignature newSignature = HashCalculator.getObjectSignature(objectIdentifier.getType(), definition);
                ret.put(objectIdentifier, newSignature);
            }
        });
        return new UpdatedSignatures(ret, lastModifiedDate[0]);
    }
}
