package org.dandoy.dbpopd.code;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpopd.utils.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.*;

@Slf4j
public class DbToFileVisitor implements AutoCloseable, DatabaseVisitor {
    private final DatabaseIntrospector introspector;
    private final File directory;
    private final Set<File> existingFiles;
    private final Map<CodeDB.TimestampObject, Timestamp> timestampMap;
    private final List<Dependency> dependencies = new ArrayList<>();
    @Getter
    private final Map<String, Integer> typeCounts = new HashMap<>();
    private final List<ObjectIdentifier> toFetch = new ArrayList<>();

    public DbToFileVisitor(DatabaseIntrospector introspector, File directory, @Nullable Map<CodeDB.TimestampObject, Timestamp> timestampMap) {
        this.introspector = introspector;
        this.directory = directory;
        this.existingFiles = CodeUtils.getCodeFiles(directory);
        this.timestampMap = timestampMap;
    }

    @Override
    public void close() {
        FileUtils.deleteFiles(existingFiles);
    }

    @Override
    public void catalog(String catalog) {
        if ("tempdb".equals(catalog)) return;
        // Write the modules to file
        introspector.visitModuleMetas(this, catalog);
        introspector.visitModuleDefinitions(this, toFetch);
        toFetch.clear();

        // Collect the dependencies
        dependencies.clear();
        introspector.visitDependencies(this, catalog);

        if (!dependencies.isEmpty()) {
            Collections.sort(dependencies);
            // Write dependencies.json
            File dependencyFile = FileUtils.toFile(directory, catalog, "dependencies.json");
            try (BufferedWriter bufferedWriter = Files.newBufferedWriter(dependencyFile.toPath())) {
                new ObjectMapper()
                        .writerWithDefaultPrettyPrinter()
                        .writeValue(bufferedWriter, dependencies);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write the dependencies to " + dependencyFile, e);
            }
        }
    }

    @Override
    public void moduleMeta(ObjectIdentifier objectIdentifier, Date modifyDate) {
        String catalog = objectIdentifier.getCatalog();
        String schema = objectIdentifier.getSchema();
        String name = objectIdentifier.getName();
        String type = objectIdentifier.getType();
        if (isDbPopObject(catalog, schema, name, type)) return;
        File sqlFile = FileUtils.toFile(directory, catalog, schema, type, name + ".sql");
        if (existingFiles.remove(sqlFile)) { // Only needed if the file exists
            if (isFileNewerThanDb(catalog, schema, name, type, modifyDate, sqlFile)) {
                return;
            }
        }
        toFetch.add(objectIdentifier);
    }

    @Override
    public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
        String catalog = objectIdentifier.getCatalog();
        String schema = objectIdentifier.getSchema();
        String name = objectIdentifier.getName();
        String type = objectIdentifier.getType();
        File sqlFile = FileUtils.toFile(directory, catalog, schema, type, name + ".sql");
        try {
            File dir = sqlFile.getParentFile();
            if (!dir.isDirectory() && !dir.mkdirs()) throw new RuntimeException("Failed to create " + dir);
            try (BufferedWriter bufferedWriter = Files.newBufferedWriter(sqlFile.toPath())) {
                bufferedWriter.write(definition);
            }
            if (!sqlFile.setLastModified(modifyDate.getTime())) {
                log.error("Failed to setLastModified on " + sqlFile);
            }
            int count = typeCounts.getOrDefault(type, 0);
            typeCounts.put(type, count + 1);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write %s".formatted(sqlFile), e);
        }
    }

    @SuppressWarnings("unused")
    private boolean isDbPopObject(String catalog, String schema, String name, String moduleType) {
        return "dbpop_timestamps".equals(name);
    }

    private boolean isFileNewerThanDb(String catalog, String schema, String name, String moduleType, Date modifyDate, File sqlFile) {
        long lastModified = sqlFile.lastModified();
        if (timestampMap != null) { // If we have a timestampMap, use it
            Timestamp dbTimestamp = timestampMap.get(new CodeDB.TimestampObject(moduleType, catalog, schema, name));
            if (dbTimestamp != null) { // The code has been uploaded by dbpop.
                return dbTimestamp.getTime() <= lastModified;
            } else {
                // The object doesn't exist in the dbpop_timestamps table, so it must have been created by the developer.
                // We still want to check the modifyDate.
            }
        }  // if we don't have a timestampMap, use the modifyDate which is the DB's timestamp

        return modifyDate.getTime() <= lastModified;
    }

    @Override
    public void dependency(String catalog, String schema, String name, String moduleType, String dependentSchema, String dependentName, String dependentModuleType) {
        dependencies.add(
                new Dependency(
                        new TableName(catalog, schema, name),
                        new TableName(catalog, dependentSchema, dependentName)
                )
        );
    }
}
