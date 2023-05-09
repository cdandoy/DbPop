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

import java.io.*;
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
    private boolean hasScannedTargetCode = false;
    private Map<ObjectIdentifier, ObjectSignature> targetObjectSignatures = new HashMap<>();

    public ChangeDetector(ConfigurationService configurationService) {
        this.configurationService = configurationService;
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
                    // The file already contains that code
                    DatabaseIntrospector databaseIntrospector = targetDatabase.createDatabaseIntrospector();
                    MessageDigest messageDigest = getMessageDigest();
                    for (String catalog : targetDatabase.getCatalogs()) {
                        databaseIntrospector.visitModuleDefinitions(catalog, new DatabaseVisitor() {
                            @Override
                            public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
                                byte[] hash = messageDigest.digest(definition.getBytes(StandardCharsets.UTF_8));
                                ObjectSignature oldSignature = targetObjectSignatures.get(objectIdentifier);
                                seen.remove(objectIdentifier);
                                if (oldSignature == null || !Arrays.equals(hash, oldSignature.hash())) {
                                    File file = DbPopdFileUtils.toFile(codeDirectory, objectIdentifier);
                                    byte[] fileHash = getFileHash(file);
                                    if (Arrays.equals(fileHash, hash)) {
                                        // The file already contains that code
                                        targetObjectSignatures.put(objectIdentifier, new ObjectSignature(modifyDate, fileHash));
                                    } else {
                                        log.info("Downloading {}", objectIdentifier.toQualifiedName());
                                        writeDefinition(file, definition);
                                        targetObjectSignatures.put(objectIdentifier, new ObjectSignature(modifyDate, hash));
                                        changeFile.objectUpdated(objectIdentifier);
                                    }
                                }
                            }
                        });
                    }
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
