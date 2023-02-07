package org.dandoy.dbpopd.code;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

@Slf4j
public class DbToFileVisitor implements AutoCloseable, DatabaseVisitor {
    final DatabaseIntrospector introspector;
    final File directory;
    private final List<Dependency> dependencies = new ArrayList<>();
    @Getter
    private final Map<String, Integer> typeCounts = new HashMap<>();

    public DbToFileVisitor(DatabaseIntrospector introspector, File directory) {
        this.introspector = introspector;
        this.directory = directory;
    }

    @Override
    public void close() {
    }

    @Override
    public void catalog(String catalog) {
        if ("tempdb".equals(catalog)) return;
        visitCode(catalog);

        // Collect the dependencies
        dependencies.clear();
        introspector.visitDependencies(this, catalog);

        if (!dependencies.isEmpty()) {
            Collections.sort(dependencies);
            // Write dependencies.json
            File dependencyFile = DbPopdFileUtils.toFile(directory, catalog, "dependencies.json");
            try (BufferedWriter bufferedWriter = Files.newBufferedWriter(dependencyFile.toPath())) {
                new ObjectMapper()
                        .writerWithDefaultPrettyPrinter()
                        .writeValue(bufferedWriter, dependencies);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write the dependencies to " + dependencyFile, e);
            }
        }
    }

    protected void visitCode(String catalog) {
        introspector.visitModuleDefinitions(this, catalog);
    }

    @Override
    public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
        String catalog = objectIdentifier.getCatalog();
        String schema = objectIdentifier.getSchema();
        String name = objectIdentifier.getName();
        String type = objectIdentifier.getType();
        File sqlFile = DbPopdFileUtils.toFile(directory, catalog, schema, type, name + ".sql");
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

    protected boolean isFileNewerThanDb(String catalog, String schema, String name, String moduleType, Date modifyDate, File sqlFile) {
        return modifyDate.getTime() <= sqlFile.lastModified();
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
