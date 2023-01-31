package org.dandoy.dbpopd.code;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpopd.utils.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

@Slf4j
public class DbToFileVisitor implements AutoCloseable, DatabaseVisitor {
    private final DatabaseIntrospector introspector;
    private final File directory;
    private final Set<File> existingFiles;
    private final List<Dependency> dependencies = new ArrayList<>();
    @Getter
    private final Map<String, Integer> typeCounts = new HashMap<>();

    public DbToFileVisitor(DatabaseIntrospector introspector, File directory) {
        this.introspector = introspector;
        this.directory = directory;
        this.existingFiles = CodeUtils.getCodeFiles(directory);
    }

    @Override
    public void close() {
        FileUtils.deleteFiles(existingFiles);
    }

    @Override
    public void catalog(String catalog) {
        if ("tempdb".equals(catalog)) return;
        // Write the modules to file
        introspector.visitModuleDefinitions(this, catalog);

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
    public void moduleDefinition(String catalog, String schema, String name, String moduleType, Date modifyDate, String definition) {
        try {
            File sqlFile = FileUtils.toFile(directory, catalog, schema, moduleType, name + ".sql");
            existingFiles.remove(sqlFile);
            File dir = sqlFile.getParentFile();
            if (!dir.isDirectory() && !dir.mkdirs()) throw new RuntimeException("Failed to create " + dir);
            try (BufferedWriter bufferedWriter = Files.newBufferedWriter(sqlFile.toPath())) {
                bufferedWriter.write(definition);
            }
            if (!sqlFile.setLastModified(modifyDate.getTime())) {
                log.error("Failed to setLastModified on " + sqlFile);
            }
            int count = typeCounts.getOrDefault(moduleType, 0);
            typeCounts.put(moduleType, count + 1);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write %s/%s/%s/%s.sql".formatted(catalog, schema, moduleType, name), e);
        }
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
