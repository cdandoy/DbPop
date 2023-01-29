package org.dandoy.dbpopd.code;

import org.dandoy.dbpop.database.TableName;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

public class CodeUtils {
    public static Set<File> getCodeFiles(File directory) {
        Set<File> ret = new HashSet<>();
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
                                    ret.add(file);
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
        return ret;
    }

    public static void toCode(File directory, File file, BiConsumer<TableName, String> consumer) {
        Path directoryPath = directory.toPath();
        Path filePath = file.toPath();
        if (!filePath.startsWith(directoryPath)) throw new RuntimeException("File is not under directory");
        int directoryCount = directoryPath.getNameCount();
        if (filePath.getNameCount() - directoryCount != 4) throw new RuntimeException("Unexpected depth");
        String catalog = filePath.getName(directoryCount).toString();
        String schema = filePath.getName(directoryCount + 1).toString();
        String type = filePath.getName(directoryCount + 2).toString();
        String filename = filePath.getName(directoryCount + 3).toString();
        if (!filename.toLowerCase().endsWith(".sql")) throw new RuntimeException("File is not a .sql file: " + file);
        String name = filename.substring(0, filename.length() - 4);
        consumer.accept(new TableName(catalog, schema, name), type);
    }
}
