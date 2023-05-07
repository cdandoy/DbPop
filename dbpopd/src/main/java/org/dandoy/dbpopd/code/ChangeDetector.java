package org.dandoy.dbpopd.code;

import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.ConfigurationService;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;

/**
 * ChangeDetector runs every 3 seconds.
 * The first run will capture the signature of all the objects in the database.
 * The subsequent runs compare the database signatures with the reference.
 * If the object has been added or changed in the database, it is written to a file.
 * If the object has been removed, the file is removed.
 * There is no attempt to detect changes made to the files.
 */
@Singleton
@Slf4j
public class ChangeDetector {
    private final ConfigurationService configurationService;
    private final ObjectSignatureService objectSignatureService;
    private boolean hasScannedTargetCode = false;
    private final Map<ObjectIdentifier, ObjectSignatureService.ObjectSignature> targetObjectSignatures = new HashMap<>();

    public ChangeDetector(ConfigurationService configurationService, ObjectSignatureService objectSignatureService) {
        this.configurationService = configurationService;
        this.objectSignatureService = objectSignatureService;
    }

    @Scheduled(fixedDelay = "3s", initialDelay = "3s")
    void checkCodeChanges() {
        if (configurationService.isCodeAutoSave()) {
            if (!hasScannedTargetCode) {
                captureObjectSignatures();
            } else {
                compareObjectSignatures();
            }
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
                try (ChangeFile changeFile = new ChangeFile(new File(codeDirectory, "changes.txt"))) {
                    objectSignatureService.visitObjectDefinitions(targetDatabase, (objectIdentifier, modifyDate, hash, definition) -> {
                        ObjectSignatureService.ObjectSignature oldSignature = targetObjectSignatures.get(objectIdentifier);
                        seen.remove(objectIdentifier);
                        if (oldSignature == null || !Arrays.equals(hash, oldSignature.hash())) {
                            File file = toFile(codeDirectory, objectIdentifier);
                            byte[] fileHash = objectSignatureService.getFileHash(file);
                            if (Arrays.equals(fileHash, hash)) {
                                // The file already contains that code
                                targetObjectSignatures.put(objectIdentifier, new ObjectSignatureService.ObjectSignature(modifyDate, fileHash));
                            } else {
                                log.info("Downloading {}", objectIdentifier.toQualifiedName());
                                writeDefinition(file, definition);
                                targetObjectSignatures.put(objectIdentifier, new ObjectSignatureService.ObjectSignature(modifyDate, hash));
                                changeFile.objectUpdated(objectIdentifier);
                            }
                        }
                    });
                    for (ObjectIdentifier removedObjectIdentifier : seen) {
                        File file = toFile(codeDirectory, removedObjectIdentifier);
                        log.info("deleting {}", file);
                        targetObjectSignatures.remove(removedObjectIdentifier);
                        changeFile.objectDeleted(removedObjectIdentifier);
                        if (!file.delete() && file.exists()) {
                            log.error("Failed to delete " + file);
                        }
                    }
                }
            }
        }
    }

    synchronized void captureObjectSignatures() {
        try (Database targetDatabase = safeGetTargetDatabase()) {
            if (targetDatabase != null) {
                Map<ObjectIdentifier, ObjectSignatureService.ObjectSignature> objectDefinitions = objectSignatureService.getObjectDefinitions(targetDatabase);
                targetObjectSignatures.putAll(objectDefinitions);
                hasScannedTargetCode = true;
            }
        }
    }

    /**
     * Safely executes the supplier without checking for code changes
     */
    synchronized <T> T holdingChanges(Supplier<T> supplier) {
        return supplier.get();
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

    private static File toFile(File directory, ObjectIdentifier objectIdentifier) {
        return directory.toPath()
                .resolve(
                        Path.of(
                                objectIdentifier.getCatalog(),
                                objectIdentifier.getSchema(),
                                objectIdentifier.getType(),
                                objectIdentifier.getName() + ".sql"
                        )
                )
                .toFile();
    }

    private static class ChangeFile implements AutoCloseable {
        private final File file;
        private Map<ObjectIdentifier, Boolean> identifiers;

        ChangeFile(File file) {
            this.file = file;
        }

        void objectUpdated(ObjectIdentifier objectIdentifier) {
            init();
            identifiers.put(objectIdentifier, false);
        }

        void objectDeleted(ObjectIdentifier objectIdentifier) {
            init();
            identifiers.put(objectIdentifier, true);
        }

        private void init() {
            if (identifiers == null) {
                identifiers = new HashMap<>();
                if (file.exists()) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
                        while (true) {
                            String line = reader.readLine();
                            if (line == null) break;

                            int split = line.indexOf(" ");
                            String deleted = line.substring(0, split);
                            line = line.substring(split + 1);

                            split = line.indexOf(" ");
                            String type = line.substring(0, split);
                            line = line.substring(split + 1);

                            split = line.indexOf(".");
                            String catalog = line.substring(0, split);
                            line = line.substring(split + 1);

                            split = line.indexOf(".");
                            String schema = line.substring(0, split);
                            String name = line.substring(split + 1);
                            identifiers.put(
                                    new ObjectIdentifier(
                                            type, catalog, schema, name
                                    ),
                                    "DELETED".equals(deleted)
                            );
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to read " + file, e);
                    }
                }
            }
        }

        @Override
        public void close() {
            if (identifiers != null) {
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
                    identifiers.keySet().stream().sorted().forEach(objectIdentifier -> {
                        Boolean deleted = identifiers.get(objectIdentifier);
                        try {
                            writer.append(deleted == Boolean.TRUE ? "DELETED" : "UPDATED")
                                    .append(" ")
                                    .append(objectIdentifier.getType())
                                    .append(" ")
                                    .append(objectIdentifier.getCatalog())
                                    .append(".")
                                    .append(objectIdentifier.getSchema())
                                    .append(".")
                                    .append(objectIdentifier.getName())
                                    .append('\n')
                            ;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write to " + file, e);
                }
            }
        }
    }
}
