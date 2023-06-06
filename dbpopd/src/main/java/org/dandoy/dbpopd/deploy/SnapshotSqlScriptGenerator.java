package org.dandoy.dbpopd.deploy;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Slf4j
public class SnapshotSqlScriptGenerator extends SnapshotScriptGenerator {

    public SnapshotSqlScriptGenerator(Database database, File snapshotFile, File codeDirectory) {
        super(database, snapshotFile, codeDirectory);
    }

    public GenerateSqlScriptsResult generateSqlScripts() {
        try {
            File deployFile = File.createTempFile("deploy", ".sql");
            File undeployFile = File.createTempFile("undeploy", ".sql");
            try {
                Map<ObjectIdentifier, Boolean> transitionedObjectIdentifier = generateSqlScripts(deployFile, undeployFile);
                File zipFile = File.createTempFile("deployment", ".zip");
                try (ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zipFile)), StandardCharsets.UTF_8)) {

                    zipOutputStream.putNextEntry(new ZipEntry("deploy.sql"));
                    FileUtils.copyFile(deployFile, zipOutputStream);

                    zipOutputStream.putNextEntry(new ZipEntry("undeploy.sql"));
                    FileUtils.copyFile(undeployFile, zipOutputStream);
                }
                return new GenerateSqlScriptsResult(zipFile, transitionedObjectIdentifier);
            } finally {
                if (deployFile.delete() && deployFile.isFile()) log.error("Failed to delete " + deployFile);
                if (undeployFile.delete() && undeployFile.isFile()) log.error("Failed to delete " + undeployFile);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    Map<ObjectIdentifier, Boolean> generateSqlScripts(File deployFile, File undeployFile) {
        try {
            List<ChangeWithSource> changeWithSources = getChangeWithSources();
            Map<ObjectIdentifier, Boolean> transitionedObjectIdentifier = new HashMap<>();

            {   // Deploy
                writeChanges(deployFile, changeWithSources, transitionedObjectIdentifier);
            }

            {   // Undeploy
                List<ChangeWithSource> reversedChangeWithSources = changeWithSources.stream()
                        .map(it -> new ChangeWithSource(it.objectIdentifier(), it.fileSql(), it.snapshotSql()))
                        .collect(Collectors.toCollection(ArrayList::new));
                Collections.reverse(reversedChangeWithSources);
                writeChanges(undeployFile, reversedChangeWithSources, transitionedObjectIdentifier);
            }

            return transitionedObjectIdentifier;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public record GenerateSqlScriptsResult(File zipFile, Map<ObjectIdentifier, Boolean> transitionedObjectIdentifier) {}
}
