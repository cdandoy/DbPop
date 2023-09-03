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
import org.dandoy.dbpop.utils.CollectionComparator;
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
    private Boolean fileScanComplete = null;
    private final Map<ObjectIdentifier, ObjectSignature> fileSignatures = new HashMap<>();
    private Map<ObjectIdentifier, ObjectSignature> databaseSignatures;
    private Date lastDatabaseCheck = new Date(0L);
    @Getter
    private SignatureDiff signatureDiff = new SignatureDiff(emptyList(), emptyList(), emptyList());
    private ConnectionBuilder targetConnectionBuilder;
    private boolean paused;
    static ObjectIdentifier debugObjectIdentifier = null;

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
        siteWebSocket.setCodeScanComplete(databaseSignatures != null && Boolean.TRUE.equals(fileScanComplete));
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
            try (DefaultDatabase database = Database.createDefaultDatabase(targetConnectionBuilder)) {
                DatabaseChangeDetector.UpdatedSignatures updatedSignatures = DatabaseChangeDetector.getUpdatedSignatures(database, lastDatabaseCheck, databaseSignatures);
                databaseSignatures = updatedSignatures.signatures();
                lastDatabaseCheck = updatedSignatures.lastModifiedDate();
            }
            compareSignatures();
        }
    }

    /**
     * FileChangeDetector only works if the code directory exists.
     */
    private void checkFileChangeDetector() {
        try {
            if (codeDirectory.exists()) {
                if (fileChangeDetector == null) {
                    ElapsedStopWatch stopWatch = new ElapsedStopWatch();
                    fileScanComplete = false;
                    fileChangeDetector = FileChangeDetector.createFileChangeDetector(codeDirectory.toPath(), this);
                    log.info("Scanned {} in {}", codeDirectory, stopWatch);
                }
            } else {
                if (fileChangeDetector != null) {
                    fileChangeDetector.close();
                    fileChangeDetector = null;
                }
            }
        } finally {
            fileScanComplete = true;
            whenScanComplete();
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
                        String sql = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
                        ObjectSignature objectSignature = HashCalculator.getObjectSignature(sql);
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

    private void compareSignatures() {
        // Take a copy to prevent side effects
        Map<ObjectIdentifier, ObjectSignature> databaseSignatures = this.databaseSignatures;
        FileChangeDetector fileChangeDetector = this.fileChangeDetector;
        Map<ObjectIdentifier, ObjectSignature> fileSignatures = this.fileSignatures;

        // Early exit if both haven't scanned
        if (databaseSignatures == null || fileChangeDetector == null) return;

        // Compare the files and database signatures
        CollectionComparator<ObjectIdentifier> comparator = CollectionComparator.build(fileSignatures.keySet(), databaseSignatures.keySet());
        Collection<ObjectIdentifier> fileOnly = comparator.leftOnly;
        Collection<ObjectIdentifier> databaseOnly = comparator.rightOnly;
        Collection<ObjectIdentifier> different = comparator.common.stream()
                .filter(objectIdentifier -> {
                    ObjectSignature fileSignature = fileSignatures.get(objectIdentifier);
                    ObjectSignature databaseSignature = databaseSignatures.get(objectIdentifier);
                    if (fileSignature == null) {
                        log.warn("File Signature disappeared - {}", objectIdentifier);
                        return false;
                    }
                    if (databaseSignature == null) {
                        log.warn("Database Signature disappeared - {}", objectIdentifier);
                        return false;
                    }
                    byte[] fileHash = fileSignature.hash();
                    byte[] databaseHash = databaseSignature.hash();
                    boolean ret = !Arrays.equals(fileHash, databaseHash);
                    if (objectIdentifier.equals(debugObjectIdentifier)) {
                        log.info("compare Signature: {} - {} - {}",
                                ret ? "different" : "identical",
                                ByteArrayUtil.toHexString(fileHash),
                                ByteArrayUtil.toHexString(databaseHash)
                        );
                    }
                    return ret;
                })
                .toList();

        // We only want to tell the client when the differences have changed.
        SignatureDiff signatureDiff = new SignatureDiff(fileOnly, databaseOnly, different);
        boolean isSameDifferences = this.signatureDiff.equals(signatureDiff);
        this.signatureDiff = signatureDiff;
        if (!isSameDifferences) {
            siteWebSocket.codeDiffChanged(signatureDiff.hasChanges());
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
