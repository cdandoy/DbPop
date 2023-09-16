package org.dandoy.dbpopd.deploy;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.code.CodeService;
import org.dandoy.dbpopd.codechanges.HashCalculator;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Slf4j
public class SnapshotComparator {
    private final File snapshotFile;
    private final File codeDirectory;
    private final DbPopdFileUtils.FileToObjectIdentifierResolver fileToObjectIdentifierResolver;

    public SnapshotComparator(File snapshotFile, File codeDirectory) {
        this.snapshotFile = snapshotFile;
        this.codeDirectory = codeDirectory;
        fileToObjectIdentifierResolver = DbPopdFileUtils.createFileToObjectIdentifierResolver(codeDirectory);
    }

    @SneakyThrows
    void consumeChanges(ChangeConsumer changeConsumer) {
        boolean keepRunning = true;

        // Get all the files in the code directory
        Set<Path> pathsByPriority = new LinkedHashSet<>(getPathsByPriority());

        // Go through each file in snapshot.zip
        // Compare it to the code directory
        // call the consumer if the content is different or if has been deleted
        // remove it from pathsByPriority
        if (snapshotFile.exists()) {
            try (ZipInputStream zipInputStream = new ZipInputStream(new BufferedInputStream(new FileInputStream(snapshotFile)))) {
                while (keepRunning) {
                    ZipEntry zipEntry = zipInputStream.getNextEntry();
                    if (zipEntry == null) break;
                    String entryName = zipEntry.getName();
                    if (entryName.endsWith(".sql")) {
                        String snapshotSql = IOUtils.toString(zipInputStream, StandardCharsets.UTF_8);

                        File file = new File(codeDirectory, entryName);
                        ObjectIdentifier objectIdentifier = DbPopdFileUtils.toObjectIdentifier(codeDirectory, file);
                        if (objectIdentifier != null) {
                            if (file.exists()) {
                                // Object CHANGED: it exists in the code directory but is different from the snapshot
                                pathsByPriority.remove(file.toPath());
                                String fileSql = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
                                String cleanSnapshotSql = HashCalculator.cleanSqlForHash(snapshotSql);
                                String cleanFileSql = HashCalculator.cleanSqlForHash(fileSql);
                                if (!cleanSnapshotSql.equals(cleanFileSql)) {
                                    keepRunning = changeConsumer.accept(objectIdentifier, snapshotSql, fileSql);
                                }
                            } else {
                                // Object DELETED: it exists in snapshot, but not in the code directory
                                keepRunning = changeConsumer.accept(objectIdentifier, snapshotSql, null);
                            }
                        } else {
                            log.warn("Unexpected file in {}: {}", snapshotFile, entryName);
                        }
                    }
                }
            }
        }

        // Call the consumer for each file in the code directory that wasn't in snapshot.zip
        DbPopdFileUtils.FileToObjectIdentifierResolver resolver = DbPopdFileUtils.createFileToObjectIdentifierResolver(codeDirectory);
        Iterator<Path> iterator = pathsByPriority.iterator();
        while (keepRunning && iterator.hasNext()) {
            Path path = iterator.next();
            File file = codeDirectory.toPath().resolve(path).toFile();
            ObjectIdentifier objectIdentifier = resolver.getObjectIdentifier(path);
            // Object ADDED: it exists in the code directory, but not in the snapshot
            String fileSql = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            keepRunning = changeConsumer.accept(objectIdentifier, null, fileSql);
        }
    }

    @SneakyThrows
    Collection<Path> getPathsByPriority() {
        Map<Integer, List<Path>> pathsByPriority = new TreeMap<>();
        if (codeDirectory.isDirectory()) {
            Path codeDirectoryPath = codeDirectory.toPath();
            Files.walkFileTree(codeDirectoryPath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                    ObjectIdentifier objectIdentifier = fileToObjectIdentifierResolver.getObjectIdentifier(path);
                    if (objectIdentifier != null) {
                        int priority = CodeService.CODE_TYPES.indexOf(objectIdentifier.getType());
                        if (priority >= 0) {
                            pathsByPriority.computeIfAbsent(priority, integer -> new ArrayList<>())
                                    .add(path);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        return pathsByPriority.values().stream().flatMap(Collection::stream).toList();
    }

    interface ChangeConsumer {
        boolean accept(ObjectIdentifier objectIdentifier, String snapshotSql, String fileSql) throws IOException;
    }
}
