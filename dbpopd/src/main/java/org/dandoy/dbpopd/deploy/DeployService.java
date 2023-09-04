package org.dandoy.dbpopd.deploy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.DatabaseCacheService;

import java.io.*;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.dandoy.dbpop.utils.FileUtils.deleteRecursively;

@Singleton
@Slf4j
public class DeployService {
    private final ConfigurationService configurationService;
    private final DatabaseCacheService databaseCacheService;
    private final File codeDirectory;

    public DeployService(DatabaseCacheService databaseCacheService, ConfigurationService configurationService) {
        this.databaseCacheService = databaseCacheService;
        codeDirectory = configurationService.getCodeDirectory();
        this.configurationService = configurationService;
    }

    SnapshotInfo getSnapshotInfo() {
        if (!configurationService.getSnapshotFile().isFile()) return new SnapshotInfo(0, DeltaType.FLYWAY);

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

    public boolean hasChanges() {
        AtomicBoolean ret = new AtomicBoolean(false);
        new SnapshotComparator(configurationService.getSnapshotFile(), codeDirectory)
                .consumeChanges((objectIdentifier, snapshotSql, fileSql) -> {
                    ret.set(true);
                    return false;
                });
        return ret.get();
    }

    public SnapshotSqlScriptGenerator.GenerateSqlScriptsResult generateSqlScripts() {
        return new SnapshotSqlScriptGenerator(
                databaseCacheService.getTargetDatabaseCacheOrThrow(),
                configurationService.getSnapshotFile(),
                configurationService.getCodeDirectory()
        ).generateSqlScripts();
    }

    public GenerateFlywayScriptsResult generateFlywayScripts(String name) {
        GenerateFlywayScriptsResult ret;

        SnapshotFlywayScriptGenerator generator = new SnapshotFlywayScriptGenerator(
                databaseCacheService.getTargetDatabaseCacheOrThrow(),
                configurationService.getSnapshotFile(),
                configurationService.getCodeDirectory(),
                configurationService.getFlywayDirectory()
        );
        ret = generator.generateFlywayScripts(name);

        createSnapshot(DeltaType.FLYWAY);

        return ret;
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
                    SnapshotComparator snapshotComparator = new SnapshotComparator(snapshotFile, codeDirectory);
                    for (Path path : snapshotComparator.getPathsByPriority()) {
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

    public void reset() {
        DeltaType deltaType = DeltaType.FLYWAY;
        // Delete snapshot.zip
        File snapshotFile = configurationService.getSnapshotFile();
        if (snapshotFile.isFile()) {
            SnapshotInfo snapshotInfo = getSnapshotInfo();
            deltaType = snapshotInfo.deltaType();

            if (!snapshotFile.delete()) {
                throw new HttpStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete " + snapshotFile);
            }
        }

        // Delete the flyway directory
        File flywayDirectory = configurationService.getFlywayDirectory();
        try {
            deleteRecursively(flywayDirectory);
        } catch (Exception e) {
            String message = "Failed to delete " + flywayDirectory;
            throw new HttpStatusException(HttpStatus.INTERNAL_SERVER_ERROR, message);
        }

        // Create a new snapshot.zip with the content of the code directory
        createSnapshot(deltaType);
    }
}
