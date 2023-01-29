package org.dandoy.dbpopd.code;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

import static org.dandoy.dbpopd.code.CodeService.CODE_TYPES;

public class CodeFileInspector {

    /**
     * Walk the codeDirectory and invokes the visitor for each catalog, schema and file
     */
    public static void inspect(File codeDirectory, CodeFileVisitor visitor) {
        File[] catalogDirs = codeDirectory.listFiles();
        if (catalogDirs != null) {
            int directoryCount = codeDirectory.toPath().getNameCount();
            for (File catalogDir : catalogDirs) {
                String catalog = catalogDir.getName();
                visitor.catalog(catalog);

                try {
                    Map<Integer, List<Path>> filesByPriority = new HashMap<>();
                    Set<String> schemas = new HashSet<>();
                    // Collect the files by priority: tables, foreign keys, indexes, stored procedures, ...
                    Files.walkFileTree(catalogDir.toPath(), new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) {
                            if (filePath.getNameCount() - directoryCount == 4) {
                                String schema = filePath.getName(directoryCount + 1).toString();
                                if (schemas.add(schema)) {
                                    visitor.schema(catalog, schema);
                                }
                                String type = filePath.getName(directoryCount + 2).toString();
                                String filename = filePath.getName(directoryCount + 3).toString();
                                if (filename.toLowerCase().endsWith(".sql")) {
                                    filesByPriority.computeIfAbsent(CODE_TYPES.indexOf(type), ArrayList::new)
                                            .add(filePath);
                                }
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
                    for (Map.Entry<Integer, List<Path>> entry : filesByPriority.entrySet()) {
                        for (Path filePath : entry.getValue()) {
                            String schema = filePath.getName(directoryCount + 1).toString();
                            String type = filePath.getName(directoryCount + 2).toString();
                            String name = filePath.getName(directoryCount + 3).toString();
                            visitor.module(catalog, schema, type, name, filePath.toFile());
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public interface CodeFileVisitor {
        void catalog(String catalog);

        void schema(String catalog, String schema);

        void module(String catalog, String schema, String type, String name, File sqlFile);
    }
}
