package org.dandoy.dbpopd.deploy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.code.ChangeDetector;
import org.dandoy.dbpopd.code.CodeService;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

@Singleton
@Slf4j
public class DeployService {
    // We do not support altering tables for example.
    public static final List<String> CAN_MODIFY = List.of("SQL_INLINE_TABLE_VALUED_FUNCTION", "SQL_SCALAR_FUNCTION", "SQL_STORED_PROCEDURE", "SQL_TABLE_VALUED_FUNCTION", "SQL_TRIGGER", "VIEW");
    private final ConfigurationService configurationService;
    private final File codeDirectory;
    private final DbPopdFileUtils.FileToObjectIdentifierResolver fileToObjectIdentifierResolver;

    public DeployService(ConfigurationService configurationService) {
        codeDirectory = configurationService.getCodeDirectory();
        this.configurationService = configurationService;
        fileToObjectIdentifierResolver = DbPopdFileUtils.createFileToObjectIdentifierResolver(codeDirectory);
    }

    SnapshotInfo getSnapshotInfo() {
        if (!configurationService.getSnapshotFile().isFile()) return null;

        File snapshotFile = configurationService.getSnapshotFile();
        try (ZipInputStream zipInputStream = new ZipInputStream(new BufferedInputStream(new FileInputStream(snapshotFile)))) {
            while (true) {
                ZipEntry zipEntry = zipInputStream.getNextEntry();
                if (zipEntry == null) return null;
                if (zipEntry.getName().equals("info.json")) {
                    return new ObjectMapper().readValue(zipInputStream, SnapshotInfo.class);
                }
            }
        } catch (Exception e) {
            log.error("Failed to get the snapshot info", e);
            return null;
        }
    }

    void withBackup(Runnable runnable) {
        File snapshotFile = configurationService.getSnapshotFile();
        File snapshotDirectory = snapshotFile.getParentFile();
        File backup = new File(snapshotDirectory, "snapshot.back");
        if (snapshotFile.isFile()) {
            if (!snapshotFile.renameTo(backup)) throw new HttpStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to backup " + snapshotFile);
        }
        try {
            runnable.run();
            // Delete backup
            if (backup.isFile() && !backup.delete()) {
                log.error("Failed to delete the backup");
            }
        } catch (Exception e) {
            // Restore the backup
            //   Delete the snapshot if it exists
            if (snapshotFile.isFile() && !snapshotFile.delete()) {
                log.error("Failed to restore the snapshot file 1");
            }
            //   Rename the backup if it exists
            if (backup.isFile() && !backup.renameTo(snapshotFile)) {
                log.error("Failed to restore the snapshot file 2");
            }

            throw e;
        }
    }

    void createSnapshot(DeltaType deltaType) {
        withBackup(() -> {
            try {
                File snapshotFile = configurationService.getSnapshotFile();
                File snapshotDirectory = snapshotFile.getParentFile();
                if (!snapshotDirectory.mkdirs() && !snapshotDirectory.isDirectory()) throw new HttpStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create the directory " + snapshotDirectory);

                try (ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
                    // Write the info file
                    zipOutputStream.putNextEntry(new ZipEntry("info.json"));
                    SnapshotInfo snapshotInfo = new SnapshotInfo(System.currentTimeMillis(), deltaType);
                    new ObjectMapper().writeValue(CloseShieldOutputStream.wrap(zipOutputStream), snapshotInfo);

                    // Get a snapshot of the code
                    Path codeDirectoryPath = codeDirectory.toPath();
                    for (Path path : getPathsByPriority()) {
                        String pathName = codeDirectoryPath.relativize(path).toString();
                        zipOutputStream.putNextEntry(new ZipEntry(pathName));
                        try (FileInputStream fileInputStream = new FileInputStream(path.toFile())) {
                            IOUtils.copy(fileInputStream, zipOutputStream);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @SneakyThrows
    private Collection<Path> getPathsByPriority() {
        Map<Integer, Path> pathsByPriority = new TreeMap<>();
        if (codeDirectory.isDirectory()) {
            Path codeDirectoryPath = codeDirectory.toPath();
            Files.walkFileTree(codeDirectoryPath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                    ObjectIdentifier objectIdentifier = fileToObjectIdentifierResolver.getObjectIdentifier(path);
                    if (objectIdentifier != null) {
                        int priority = CodeService.CODE_TYPES.indexOf(objectIdentifier.getType());
                        if (priority >= 0) {
                            pathsByPriority.put(priority, path);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        return pathsByPriority.values();
    }

    interface ChangeConsumer {
        boolean accept(ObjectIdentifier objectIdentifier, String snapshotSql, String fileSql) throws IOException;
    }

    @SneakyThrows
    private void consumeChanges(ChangeConsumer changeConsumer) {
        boolean keepRunning = true;
        Set<Path> pathsByPriority = new LinkedHashSet<>(getPathsByPriority());
        File snapshotFile = configurationService.getSnapshotFile();
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
                        if (CAN_MODIFY.contains(objectIdentifier.getType())) {
                            if (file.exists()) {
                                pathsByPriority.remove(file.toPath());
                                String fileSql = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
                                String cleanSnapshotSql = ChangeDetector.cleanSql(snapshotSql);
                                String cleanFileSql = ChangeDetector.cleanSql(fileSql);
                                if (!cleanSnapshotSql.equals(cleanFileSql)) {
                                    keepRunning = changeConsumer.accept(objectIdentifier, snapshotSql, fileSql);
                                }
                            } else {
                                keepRunning = changeConsumer.accept(objectIdentifier, snapshotSql, null);
                            }
                        }
                    } else {
                        log.warn("Unexpected file in {}: {}", snapshotFile, entryName);
                    }
                }
            }
        }
        DbPopdFileUtils.FileToObjectIdentifierResolver resolver = DbPopdFileUtils.createFileToObjectIdentifierResolver(codeDirectory);
        Iterator<Path> iterator = pathsByPriority.iterator();
        while (keepRunning && iterator.hasNext()) {
            Path path = iterator.next();
            File file = codeDirectory.toPath().resolve(path).toFile();
            ObjectIdentifier objectIdentifier = resolver.getObjectIdentifier(path);
            if (CAN_MODIFY.contains(objectIdentifier.getType())) {
                String fileSql = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
                keepRunning = changeConsumer.accept(objectIdentifier, null, fileSql);
            }
        }
    }

    public File generateSqlScripts() {
        try {
            File deployFile = File.createTempFile("deploy", ".sql");
            File undeployFile = File.createTempFile("undeploy", ".sql");
            try {
                generateSqlScripts(deployFile, undeployFile);
                File zipFile = File.createTempFile("deployment", ".zip");
                try (ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zipFile)), StandardCharsets.UTF_8)) {

                    zipOutputStream.putNextEntry(new ZipEntry("deploy.sql"));
                    FileUtils.copyFile(deployFile, zipOutputStream);

                    zipOutputStream.putNextEntry(new ZipEntry("undeploy.sql"));
                    FileUtils.copyFile(undeployFile, zipOutputStream);
                }
                return zipFile;
            } finally {
                if (deployFile.delete() && deployFile.isFile()) log.error("Failed to delete " + deployFile);
                if (undeployFile.delete() && undeployFile.isFile()) log.error("Failed to delete " + undeployFile);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasChanges() {
        AtomicBoolean ret = new AtomicBoolean(false);
        consumeChanges((objectIdentifier, snapshotSql, fileSql) -> {
            ret.set(true);
            return false;
        });
        return ret.get();
    }

    private void generateSqlScripts(File deployFile, File undeployFile) {
        try {
            Database targetDatabase = configurationService.getTargetDatabaseCache();
            try (BufferedWriter deployWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(deployFile), StandardCharsets.UTF_8))) {
                try (BufferedWriter undeployWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(undeployFile), StandardCharsets.UTF_8))) {
                    consumeChanges((objectIdentifier, snapshotSql, fileSql) -> {
                        appendSql(targetDatabase, deployWriter, objectIdentifier, fileSql);
                        appendSql(targetDatabase, undeployWriter, objectIdentifier, snapshotSql);
                        return true;
                    });
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void appendSql(Database database, Writer writer, ObjectIdentifier objectIdentifier, String sql) throws IOException {
        if (sql == null) {
            sql = switch (objectIdentifier.getType()) {
                case "SQL_STORED_PROCEDURE" -> "DROP PROCEDURE IF EXISTS " + database.quote(".", objectIdentifier.getSchema(), objectIdentifier.getName());
                case "SQL_INLINE_TABLE_VALUED_FUNCTION" -> "DROP FUNCTION IF EXISTS " + database.quote(".", objectIdentifier.getSchema(), objectIdentifier.getName());
                default -> null;
            };
        }
        if (sql != null) {
            writer.append(sql).append("\nGO\n");
        }
    }

    public String generateFlywayScripts(String name) {
        File flywayFile = getNextFlywayFile(name);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(flywayFile), StandardCharsets.UTF_8))) {
            Database targetDatabase = configurationService.getTargetDatabaseCache();
            consumeChanges((objectIdentifier, snapshotSql, fileSql) -> {
                appendSql(targetDatabase, writer, objectIdentifier, fileSql);
                return true;
            });
            createSnapshot(DeltaType.FLYWAY);

            String filename = flywayFile.toString();
            if (filename.startsWith("/var/opt/dbpop/")) {
                filename = filename.substring("/var/opt/dbpop/".length());
            }
            return filename;
        } catch (Exception e) {
            if (!flywayFile.delete() && flywayFile.isFile()) {
                log.error("Failed to delete " + flywayFile);
            }
        }
        return null;
    }

    private static final Pattern FLYWAY_PATTERN = Pattern.compile("V(\\d+)__(.*).sql");

    private File getNextFlywayFile(String name) {
        int flywayIndex = getLastFlywayIndex() + 1;
        String filename = "V%d__%s.sql".formatted(flywayIndex, name);
        return new File(configurationService.getFlywayDirectory(), filename);
    }

    private int getLastFlywayIndex() {
        File flywayDirectory = configurationService.getFlywayDirectory();
        if (flywayDirectory.isDirectory()) {
            String[] list = flywayDirectory.list();
            if (list != null) {
                return Arrays.stream(list)
                        .map(name -> {
                            Matcher matcher = FLYWAY_PATTERN.matcher(name);
                            if (matcher.matches()) {
                                return Integer.parseInt(matcher.group(1));
                            }
                            return null;
                        })
                        .filter(Objects::nonNull)
                        .mapToInt(Integer::intValue)
                        .max()
                        .orElse(0);
            }
        } else {
            if (!flywayDirectory.mkdirs()) {
                throw new RuntimeException("Failed to create the directory " + flywayDirectory);
            }
        }
        return 0;
    }
}
