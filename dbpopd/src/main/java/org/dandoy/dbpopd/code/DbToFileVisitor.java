package org.dandoy.dbpopd.code;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.mssql.SqlServerDatabaseIntrospector;
import org.dandoy.dbpop.database.mssql.SqlServerDatabaseVisitor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Date;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class DbToFileVisitor implements AutoCloseable, SqlServerDatabaseVisitor {
    private final DatabaseIntrospector introspector;
    private final File directory;
    private final Set<File> existingFiles = new HashSet<>();

    public DbToFileVisitor(DatabaseIntrospector introspector, File directory) {
        this.introspector = introspector;
        this.directory = directory;
        if (directory.isDirectory()) {
            try {
                Path start = directory.toPath();
                int baseNameCount = start.getNameCount();
                Files.walkFileTree(start, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                        int nameCount = path.getNameCount();
                        if (nameCount - baseNameCount == 4) {
                            String typeName = path.getName(baseNameCount + 2).toString();
                            if (CodeService.CODE_TYPES.contains(typeName)) {
                                File file = path.toFile();
                                if (file.getName().endsWith(".sql")) {
                                    existingFiles.add(file);
                                }
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() {
        for (File file : existingFiles) {
            if (!file.delete() && file.exists()) {
                log.error("Failed to delete " + file);
            }
        }
    }

    @Override
    public void catalog(String catalog) {
        if (introspector instanceof SqlServerDatabaseIntrospector sqlServerIntrospector) {
            sqlServerIntrospector.visitModuleDefinitions(this, catalog);
        }
    }

    private File toFile(String... parts) {
        return new File(directory, String.join("/", parts));
    }

    @Override
    public void moduleDefinition(String catalog, String schema, String name, String moduleType, Date modifyDate, String definition) {
        File sqlFile = toFile(catalog, schema, moduleType, name + ".sql");
        existingFiles.remove(sqlFile);
        File dir = sqlFile.getParentFile();
        if (!dir.isDirectory() && !dir.mkdirs()) throw new RuntimeException("Failed to create " + dir);
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(sqlFile.toPath())) {
            bufferedWriter.write(definition);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (!sqlFile.setLastModified(modifyDate.getTime())) {
            log.error("Failed to setLastModified on " + sqlFile);
        }
    }
}
