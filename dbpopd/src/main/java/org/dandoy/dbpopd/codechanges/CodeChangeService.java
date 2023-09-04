package org.dandoy.dbpopd.codechanges;

import ch.qos.logback.core.encoder.ByteArrayUtil;
import io.micronaut.context.annotation.Context;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DefaultDatabase;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.utils.ElapsedStopWatch;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.ConnectionBuilderChangedEvent;
import org.dandoy.dbpopd.config.ConnectionType;
import org.dandoy.dbpopd.site.SiteWebSocket;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

@Singleton
@Slf4j
@Context
public class CodeChangeService implements FileChangeDetector.FileChangeListener {
    private final File codeDirectory;
    private final SiteWebSocket siteWebSocket;
    private FileChangeDetector fileChangeDetector;
    private boolean fileScanComplete = true;
    /**
     * We only send the comparison of files and database signatures when we have them both,
     * but we don't want to ignore any changes that were detected in the database while the files were scanned
     */
    private boolean compareSent = false;
    private final Map<ObjectIdentifier, ObjectSignature> fileSignatures = new HashMap<>();
    private Map<ObjectIdentifier, ObjectSignature> databaseSignatures;
    private Date lastDatabaseCheck = new Date(0L);
    @Getter
    private SignatureDiff signatureDiff = new SignatureDiff(emptyList(), emptyList(), emptyList(), emptyList(), emptyList());
    private ConnectionBuilder targetConnectionBuilder;
    private boolean paused;
    static ObjectIdentifier debugObjectIdentifier = new ObjectIdentifier("SQL_STORED_PROCEDURE", "master", "dbo", "test");

    public CodeChangeService(ConfigurationService configurationService, SiteWebSocket siteWebSocket) {
        this.codeDirectory = configurationService.getCodeDirectory();
        this.siteWebSocket = siteWebSocket;
    }

    @EventListener
    void receiveConnectionBuilderChangedEvent(ConnectionBuilderChangedEvent event) {
        if (event.type() == ConnectionType.TARGET) {
            ConnectionBuilder targetConnectionBuilder = event.connectionBuilder();
            if (targetConnectionBuilder == null) {
                databaseSignatures = null;
                lastDatabaseCheck = new Date(0);
                this.targetConnectionBuilder = null;
            } else {
                Thread thread = new Thread(() -> {
                    // If we have a new database connection, get all the signatures
                    ElapsedStopWatch stopWatch = new ElapsedStopWatch();
                    try (DefaultDatabase database = Database.createDefaultDatabase(targetConnectionBuilder)) {
                        DatabaseChangeDetector.UpdatedSignatures updatedSignatures = DatabaseChangeDetector.getAllSignatures(database);
                        databaseSignatures = updatedSignatures.signatures();
                        lastDatabaseCheck = updatedSignatures.lastModifiedDate();
                        this.targetConnectionBuilder = targetConnectionBuilder;
                        whenScanComplete();
                    } finally {
                        log.info("Scanned the database code in {}", stopWatch);
                    }
                }, "TargetScanner");
                thread.setDaemon(true);
                thread.start();
            }
        }
    }

    private void whenScanComplete() {
        siteWebSocket.setCodeScanComplete(databaseSignatures != null && fileScanComplete);
    }

    @Scheduled(fixedDelay = "3s")
    void checkChanges() {
        checkFileChangeDetector();

        updateDatabaseSignatures();
    }

    /**
     * Pauses the file and database change scanning while the supplier is executing
     * TODO: Re-scan when it completes
     */
    public <T> T doWithPause(Supplier<T> supplier) {
        if (paused) return null;

        log.debug("paused");
        paused = true;
        try {
            return supplier.get();
        } finally {
            paused = false;
            log.debug("unpaused");
            fileScanComplete = false;
            try {
                fileChangeDetector.checkAll();
            } finally {
                fileScanComplete = true;
            }
        }
    }

    private void updateDatabaseSignatures() {
        if (!paused && targetConnectionBuilder != null) {
            boolean updated;
            try (DefaultDatabase database = Database.createDefaultDatabase(targetConnectionBuilder)) {
                DatabaseChangeDetector databaseChangeDetector = DatabaseChangeDetector.getUpdatedSignatures(database, lastDatabaseCheck, databaseSignatures);
                DatabaseChangeDetector.UpdatedSignatures updatedSignatures = databaseChangeDetector.getUpdatedSignatures();
                updated = databaseChangeDetector.isUpdated();
                databaseSignatures = updatedSignatures.signatures();
                lastDatabaseCheck = updatedSignatures.lastModifiedDate();
            }
            if (updated || !compareSent) {
                compareSignatures();
            }
        }
    }

    /**
     * FileChangeDetector only works if the code directory exists.
     */
    private void checkFileChangeDetector() {
        if (codeDirectory.exists()) {
            if (fileChangeDetector == null) {
                ElapsedStopWatch stopWatch = new ElapsedStopWatch();
                fileScanComplete = true;
                compareSent = false;
                fileChangeDetector = FileChangeDetector.createFileChangeDetector(codeDirectory.toPath(), this);
                log.info("Scanned {} in {}", codeDirectory, stopWatch);
                whenScanComplete();
            }
        } else {
            if (fileChangeDetector != null) {
                fileChangeDetector.close();
                fileChangeDetector = null;
                fileSignatures.clear();
                compareSent = false;
                fileScanComplete = true;
                whenScanComplete();
            }
        }
    }

    @Override
    public synchronized void whenFilesChanged(Map<File, Boolean> changes) {
        if (paused) return;

        for (Map.Entry<File, Boolean> entry : changes.entrySet()) {
            File file = entry.getKey();
            ObjectIdentifier objectIdentifier = DbPopdFileUtils.toObjectIdentifier(codeDirectory, file);
            if (objectIdentifier != null) {
                Boolean deleted = entry.getValue();
                if (deleted) {
                    fileSignatures.remove(objectIdentifier);
                } else {
                    try {
                        long ts = file.lastModified();
                        String sql = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
                        ObjectSignature objectSignature = HashCalculator.getObjectSignature(ts, sql);
                        if (objectIdentifier.equals(debugObjectIdentifier)) {
                            log.info("File Signature {} | {} | [{}]",
                                    objectIdentifier,
                                    ByteArrayUtil.toHexString(objectSignature.hash()),
                                    sql
                            );
                        }
                        fileSignatures.put(objectIdentifier, objectSignature);
                    } catch (IOException e) {
                        log.error("Failed to read " + file, e);
                    }
                }
            }
        }

        compareSignatures();
    }

    private static void logDebugObjectIdentifier(ObjectIdentifier objectIdentifier, ObjectSignature fileSignature, ObjectSignature databaseSignature, String type) {
        if (objectIdentifier.equals(debugObjectIdentifier)) {
            log.info("compare Signature: {} - {} - {}",
                    type,
                    ByteArrayUtil.toHexString(fileSignature.hash()),
                    ByteArrayUtil.toHexString(databaseSignature.hash())
            );
        }
    }

    private void compareSignatures() {
        // Early exit if both haven't scanned
        if (databaseSignatures == null || !fileScanComplete) return;

        // Take a copy
        Map<ObjectIdentifier, ObjectSignature> databaseSignatures = new HashMap<>(this.databaseSignatures);
        Map<ObjectIdentifier, ObjectSignature> fileSignatures = new HashMap<>(this.fileSignatures);

        Collection<ObjectIdentifier> fileOnly;
        Collection<ObjectIdentifier> databaseOnly = new HashSet<>();
        Collection<ObjectIdentifier> fileNewer = new HashSet<>();
        Collection<ObjectIdentifier> databaseNewer = new HashSet<>();
        Collection<ObjectIdentifier> different = new HashSet<>();

        // Go through every database object, and compare it to its file equivalent
        for (Map.Entry<ObjectIdentifier, ObjectSignature> databaseEntry : databaseSignatures.entrySet()) {
            ObjectIdentifier objectIdentifier = databaseEntry.getKey();
            ObjectSignature databaseSignature = databaseEntry.getValue();
            ObjectSignature fileSignature = fileSignatures.remove(objectIdentifier);

            if (fileSignature != null) {
                byte[] fileHash = fileSignature.hash();
                byte[] databaseHash = databaseSignature.hash();

                if (Arrays.equals(fileHash, databaseHash)) {
                    // Hash codes are identical, nothing to do except maybe to log
                    logDebugObjectIdentifier(objectIdentifier, fileSignature, databaseSignature, "identical");
                } else {
                    // The hash codes are different. Compare the timestamps
                    long tsDelta = databaseSignature.ts() - fileSignature.ts();
                    if (-10 <= tsDelta && tsDelta <= 10) {    // If the timestamp delta is within 10ms, we can't tell which one is which
                        logDebugObjectIdentifier(objectIdentifier, fileSignature, databaseSignature, "different");
                        different.add(objectIdentifier);
                    } else if (tsDelta > 0) {
                        logDebugObjectIdentifier(objectIdentifier, fileSignature, databaseSignature, "databaseNewer");
                        databaseNewer.add(objectIdentifier);
                    } else {
                        logDebugObjectIdentifier(objectIdentifier, fileSignature, databaseSignature, "fileNewer");
                        fileNewer.add(objectIdentifier);
                    }
                    if (objectIdentifier.equals(debugObjectIdentifier)) {
                        log.info("compare Signature: different - {} - {}",
                                ByteArrayUtil.toHexString(fileHash),
                                ByteArrayUtil.toHexString(databaseHash)
                        );
                    }
                }
            } else {
                databaseOnly.add(objectIdentifier);
            }
        }

        // What's left in fileSignature is FILE_ONLY
        fileOnly = new HashSet<>(fileSignatures.keySet());

        // We only want to tell the client when the differences have changed.
        SignatureDiff signatureDiff = new SignatureDiff(fileOnly, databaseOnly, fileNewer, databaseNewer, different);
        boolean isSameDifferences = this.signatureDiff.equals(signatureDiff);
        this.signatureDiff = signatureDiff;
        compareSent = true;
        if (!isSameDifferences) {
            siteWebSocket.codeDiffChanged(signatureDiff.hasChanges());
        }
    }

    record SignatureDiff(
            Collection<ObjectIdentifier> fileOnly,
            Collection<ObjectIdentifier> databaseOnly,
            Collection<ObjectIdentifier> fileNewer,
            Collection<ObjectIdentifier> databaseNewer,
            Collection<ObjectIdentifier> different
    ) {
        boolean hasChanges() {
            return !(fileOnly.isEmpty() && databaseOnly.isEmpty() && fileNewer.isEmpty() && databaseNewer.isEmpty() && different.isEmpty());
        }

        boolean isFileOnly(ObjectIdentifier objectIdentifier) {
            return fileOnly.contains(objectIdentifier);
        }

        boolean isDatabaseOnly(ObjectIdentifier objectIdentifier) {
            return databaseOnly.contains(objectIdentifier);
        }

        boolean isFileNewer(ObjectIdentifier objectIdentifier) {
            return fileNewer.contains(objectIdentifier);
        }

        boolean isDatabaseNewer(ObjectIdentifier objectIdentifier) {return databaseNewer.contains(objectIdentifier);}

        boolean isDifferent(ObjectIdentifier objectIdentifier) {
            return different.contains(objectIdentifier);
        }
    }
}
