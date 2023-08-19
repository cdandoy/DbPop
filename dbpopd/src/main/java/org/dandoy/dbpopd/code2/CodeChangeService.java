package org.dandoy.dbpopd.code2;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.database.DatabaseCache;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.utils.CollectionComparator;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.ConnectionType;
import org.dandoy.dbpopd.config.DatabaseCacheChangedEvent;
import org.dandoy.dbpopd.site.SiteWebSocket;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.util.Collections.emptyList;

@Singleton
@Slf4j
@Context
public class CodeChangeService implements FileChangeDetector.FileChangeListener, ApplicationEventListener<DatabaseCacheChangedEvent> {
    private final File codeDirectory;
    private final SiteWebSocket siteWebSocket;
    private FileChangeDetector fileChangeDetector;
    private final Map<ObjectIdentifier, ObjectSignature> fileSignatures = new HashMap<>();
    private Map<ObjectIdentifier, ObjectSignature> databaseSignatures;
    private Date lastDatabaseCheck;
    private DatabaseCache databaseCache;
    @Getter
    private SignatureDiff signatureDiff = new SignatureDiff(emptyList(), emptyList(), emptyList());

    public CodeChangeService(ConfigurationService configurationService, SiteWebSocket siteWebSocket) {
        this.codeDirectory = configurationService.getCodeDirectory();
        this.siteWebSocket = siteWebSocket;
    }

    @PostConstruct
    void postConstruct() {
        checkFileChangeDetector();
    }

    @Override
    public void onApplicationEvent(DatabaseCacheChangedEvent event) {
        if (event.type() == ConnectionType.TARGET) {
            databaseCache = event.databaseCache();
            if (databaseCache != null) {
                lastDatabaseCheck = new Date();
                databaseSignatures = DatabaseChangeDetector.getAllSignatures(databaseCache);
                compareSignatures();
            } else {
                databaseSignatures = null;
            }
        }
    }

    @Scheduled(fixedDelay = "3s", initialDelay = "3s")
    void checkChanges() {
        checkFileChangeDetector();

        updateDatabaseSignatures();
    }

    private void updateDatabaseSignatures() {
        if (databaseCache != null) {
            Date now = new Date();
            databaseSignatures = DatabaseChangeDetector.getUpdatedSignatures(databaseCache, lastDatabaseCheck, databaseSignatures);
            lastDatabaseCheck = now;
            compareSignatures();
        }
    }

    /**
     * FileChangeDetector only works if the code directory exists.
     */
    private void checkFileChangeDetector() {
        if (codeDirectory.exists()) {
            if (fileChangeDetector == null) {
                fileChangeDetector = FileChangeDetector.createFileChangeDetector(codeDirectory.toPath(), this);
            }
        } else {
            if (fileChangeDetector != null) {
                fileChangeDetector.close();
                fileChangeDetector = null;
            }
        }
    }

    @Override
    public synchronized void whenFilesChanged(Map<File, Boolean> changes) {
        for (Map.Entry<File, Boolean> entry : changes.entrySet()) {
            File file = entry.getKey();
            ObjectIdentifier objectIdentifier = DbPopdFileUtils.toObjectIdentifier(codeDirectory, file);
            if (objectIdentifier != null) {
                Boolean deleted = entry.getValue();
                if (deleted) {
                    fileSignatures.remove(objectIdentifier);
                } else {
                    try {
                        String sql = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
                        fileSignatures.put(objectIdentifier, HashCalculator.getObjectSignature(objectIdentifier.getType(), sql));
                    } catch (IOException e) {
                        log.error("Failed to read " + file, e);
                    }
                }
            }
        }

        compareSignatures();
    }

    private synchronized void compareSignatures() {
        // Compare the files and database signatures
        CollectionComparator<ObjectIdentifier> comparator = CollectionComparator.build(
                fileSignatures.keySet(),
                databaseSignatures == null ? Collections.emptySet() : databaseSignatures.keySet()
        );
        Collection<ObjectIdentifier> fileOnly = comparator.leftOnly;
        Collection<ObjectIdentifier> databaseOnly = comparator.rightOnly;
        Collection<ObjectIdentifier> different = comparator.common.stream()
                .filter(objectIdentifier -> {
                    ObjectSignature fileSignature = fileSignatures.get(objectIdentifier);
                    ObjectSignature databaseSignature = databaseSignatures.get(objectIdentifier);
                    byte[] fileHash = fileSignature.hash();
                    byte[] databaseHash = databaseSignature.hash();
                    return !Arrays.equals(fileHash, databaseHash);
                })
                .toList();

        // We need to tell the client that there is a difference between the filesystem and the database,
        // but we only want to tell the client everytime the differences have changed.
        // The reason is that lightbulb needs to be refreshed if there is a change, but the diff page needs to refresh everytime there is a change.
        // For example:
        //   o One sproc is different => Send a message.
        //   o Same sproc changed again => Do not send a message.
        //   o A second sproc changes => Send a message.
        SignatureDiff signatureDiff = new SignatureDiff(fileOnly, databaseOnly, different);
        boolean hasChanged = !this.signatureDiff.equals(signatureDiff);
        this.signatureDiff = signatureDiff;
        if (hasChanged) {
            siteWebSocket.codeDiffChanged(
                    fileChangeDetector != null && databaseSignatures != null && signatureDiff.hasChanges()
            );
        }
    }

    record SignatureDiff(Collection<ObjectIdentifier> fileOnly, Collection<ObjectIdentifier> databaseOnly, Collection<ObjectIdentifier> different) {
        boolean hasChanges() {
            return !(fileOnly.isEmpty() && databaseOnly.isEmpty() && different.isEmpty());
        }

        boolean isFileOnly(ObjectIdentifier objectIdentifier) {
            return fileOnly.contains(objectIdentifier);
        }

        boolean isDatabaseOnly(ObjectIdentifier objectIdentifier) {
            return databaseOnly.contains(objectIdentifier);
        }

        boolean isDifferent(ObjectIdentifier objectIdentifier) {
            return different.contains(objectIdentifier);
        }
    }
}
