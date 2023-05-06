package org.dandoy.dbpopd.code;

import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Singleton
@Slf4j
public class ObjectSignatureService {

    public interface ObjectSignatureVisitor {
        void moduleSignature(ObjectIdentifier objectIdentifier, Date modifyDate, byte[] hash, String definition);
    }

    public Map<ObjectIdentifier, ObjectSignature> getObjectDefinitions(Database database) {
        Map<ObjectIdentifier, ObjectSignature> ret = new HashMap<>();
        visitObjectDefinitions(database, (objectIdentifier, modifyDate, hash, definition) -> ret.put(objectIdentifier, new ObjectSignature(modifyDate, hash)));
        return ret;
    }

    public void visitObjectDefinitions(Database database, ObjectSignatureVisitor visitor) {
        DatabaseIntrospector databaseIntrospector = database.createDatabaseIntrospector();
        MessageDigest messageDigest = getMessageDigest();
        for (String catalog : database.getCatalogs()) {
            databaseIntrospector.visitModuleDefinitions(new DatabaseVisitor() {
                @Override
                public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
                    byte[] hash = messageDigest.digest(definition.getBytes(StandardCharsets.UTF_8));
                    visitor.moduleSignature(objectIdentifier, modifyDate, hash, definition);

                }
            }, catalog);
        }
    }

    @SneakyThrows
    private static MessageDigest getMessageDigest() {
        return MessageDigest.getInstance("SHA-256");
    }

    @SneakyThrows
    public Map<ObjectIdentifier, ObjectSignature> getObjectDefinitions(File directory) {
        Map<ObjectIdentifier, ObjectSignature> ret = new HashMap<>();
        MessageDigest messageDigest = getMessageDigest();
        Path directoryPath = directory.toPath();
        int directoryPathNameCount = directoryPath.getNameCount();
        Files.walkFileTree(directoryPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                if (path.getNameCount() == directoryPathNameCount + 4) {
                    String catalog = path.getName(directoryPathNameCount).toString();
                    String schema = path.getName(directoryPathNameCount + 1).toString();
                    String objectType = path.getName(directoryPathNameCount + 2).toString();
                    String filename = path.getName(directoryPathNameCount + 3).toString();
                    if (filename.endsWith(".sql")) {
                        String objectName = filename.substring(0, filename.length() - 4);
                        File file = path.toFile();
                        byte[] bytes = IOUtils.toBytes(file);
                        byte[] hash = messageDigest.digest(bytes);
                        ret.put(
                                new ObjectIdentifier(
                                        objectType,
                                        catalog,
                                        schema,
                                        objectName
                                ),
                                new ObjectSignature(
                                        new Date(file.lastModified()),
                                        hash
                                )
                        );
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return ret;
    }

    private static File toFile(File directory, ObjectIdentifier objectIdentifier) {
        return directory.toPath()
                .resolve(
                        Path.of(
                                objectIdentifier.getCatalog(),
                                objectIdentifier.getSchema(),
                                objectIdentifier.getType(),
                                objectIdentifier.getName() + ".sql"
                        )
                )
                .toFile();
    }

    public record ObjectSignature(Date modifyDate, byte[] hash) {}
}
