package org.dandoy.dbpopd.utils;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ObjectIdentifier;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class DbPopdFileUtils {
    public static List<File> getFiles(File dir) {
        ArrayList<File> ret = new ArrayList<>();
        getFiles(dir, ret);
        return ret;
    }

    private static void getFiles(File dir, ArrayList<File> ret) {
        File[] files = dir.listFiles();
        if (files == null) {
            ret.add(dir);
        } else {
            for (File file : files) {
                getFiles(file, ret);
            }
        }
    }

    static String toFileName(String in) {
        // Try not to create a new string: look at each character and if one is bad, then calculate the new string
        String bad = "@$%&\\/:*?\"'<>|~`#^+={}[];!";
        char[] chars = in.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            if (bad.indexOf(c) >= 0) {
                StringBuilder sb = new StringBuilder(in.substring(0, i));
                for (int j = i; j < chars.length; j++) {
                    char c2 = chars[j];
                    if (bad.indexOf(c2) >= 0) {
                        sb.append('_');
                    } else {
                        sb.append(c2);
                    }
                }
                return sb.toString();
            }
        }
        return in;
    }

    public static File toFile(File directory, String... parts) {
        return new File(
                directory,
                Arrays.stream(parts)
                        .map(DbPopdFileUtils::toFileName)
                        .collect(Collectors.joining("/"))
        );
    }

    public static File toFile(File directory, ObjectIdentifier objectIdentifier) {
        return switch (objectIdentifier.getType()) {
            case "INDEX", "FOREIGN_KEY_CONSTRAINT" -> toFile(directory,
                    objectIdentifier.getCatalog(),
                    objectIdentifier.getSchema(),
                    objectIdentifier.getType(),
                    objectIdentifier.getParent().getName(),
                    objectIdentifier.getName() + ".sql");
            default -> toFile(directory,
                    objectIdentifier.getCatalog(),
                    objectIdentifier.getSchema(),
                    objectIdentifier.getType(),
                    objectIdentifier.getName() + ".sql");
        };
    }

    public static String toFileName(String... parts) {
        return Arrays.stream(parts)
                .map(DbPopdFileUtils::toFileName)
                .collect(Collectors.joining("/"));
    }

    public static String toFileName(ObjectIdentifier objectIdentifier) {
        return switch (objectIdentifier.getType()) {
            case "INDEX", "FOREIGN_KEY_CONSTRAINT" -> toFileName(
                    objectIdentifier.getCatalog(),
                    objectIdentifier.getSchema(),
                    objectIdentifier.getType(),
                    objectIdentifier.getParent().getName(),
                    objectIdentifier.getName() + ".sql");
            default -> toFileName(
                    objectIdentifier.getCatalog(),
                    objectIdentifier.getSchema(),
                    objectIdentifier.getType(),
                    objectIdentifier.getName() + ".sql");
        };
    }

    public static ObjectIdentifier toObjectIdentifier(File directory, File file) {
        Path directoryPath = directory.toPath();
        Path path = file.toPath();
        Path relativePath = directoryPath.relativize(path);
        if (relativePath.getNameCount() == 4 || relativePath.getNameCount() == 5) {
            String catalog = relativePath.getName(0).toString();
            String schema = relativePath.getName(1).toString();
            String type = relativePath.getName(2).toString();
            String filename = relativePath.getName(relativePath.getNameCount() - 1).toString();
            if (filename.endsWith(".sql")) {
                String name = filename.substring(0, filename.length() - 4);
                if (relativePath.getNameCount() == 4) {
                    return new ObjectIdentifier(type, catalog, schema, name);
                } else {
                    String parentName = relativePath.getName(3).toString();
                    ObjectIdentifier parent = new ObjectIdentifier("USER_TABLE", catalog, schema, parentName);
                    return new ObjectIdentifier(type, catalog, schema, name, parent);
                }
            }
        }
        return null;
    }

    /**
     * Creates an object that can convert a file path to an ObjectIdentifier.
     */
    public static FileToObjectIdentifierResolver createFileToObjectIdentifierResolver(File directory) {
        return new FileToObjectIdentifierResolver(directory);
    }

    public static class FileToObjectIdentifierResolver {
        private final int directoryCount;

        public FileToObjectIdentifierResolver(File directory) {
            directoryCount = directory.toPath().getNameCount();
        }

        public ObjectIdentifier getObjectIdentifier(Path filePath) {
            int nameCount = filePath.getNameCount();
            String filename = filePath.getName(nameCount - 1).toString();
            if (filename.toLowerCase().endsWith(".sql")) {
                int depth = nameCount - directoryCount;
                if (depth == 4 || depth == 5) {
                    String catalog = filePath.getName(directoryCount).toString();
                    String schema = filePath.getName(directoryCount + 1).toString();
                    String type = filePath.getName(directoryCount + 2).toString();

                    String objectName = filename.substring(0, filename.length() - 4); // filename without ".sql"
                    if (depth == 4) {
                        return new ObjectIdentifier(type, catalog, schema, objectName);
                    } else {
                        String tableName = filePath.getName(directoryCount + 3).toString();
                        return new ObjectIdentifier(type, catalog, schema, objectName, new ObjectIdentifier("USER_TABLE", catalog, schema, tableName));
                    }

                }
            }
            return null;
        }
    }
}
