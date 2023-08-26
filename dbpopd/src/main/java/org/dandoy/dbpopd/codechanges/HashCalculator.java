package org.dandoy.dbpopd.codechanges;

import jakarta.annotation.Nonnull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.dandoy.dbpop.database.ObjectTypes.*;
import static org.dandoy.dbpop.database.mssql.SqlServerObjectTypes.*;

@Slf4j
public class HashCalculator {
    private static final Pattern SPROC_PATTERN = Pattern.compile("(?<pre>.*)\\bCREATE\\b +(OR +ALTER +)?(?<type>FUNCTION|PROC|PROCEDURE|TRIGGER|VIEW)\\b(?<post>.*)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);

    private static void eachFile(File dir, Consumer<File> fileConsumer) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                fileConsumer.accept(file);
            }
        }
    }

    public static Map<ObjectIdentifier, ObjectSignature> captureSignatures(File codeDirectory) {
        Map<ObjectIdentifier, ObjectSignature> ret = new HashMap<>();
        eachFile(codeDirectory, catalogFile -> {
            String catalog = catalogFile.getName();
            eachFile(catalogFile, schemaFile -> {
                String schema = schemaFile.getName();
                eachFile(schemaFile, objectTypeFile -> {
                    String objectType = objectTypeFile.getName();
                    switch (objectType) {
                        case USER_TABLE,
                                VIEW,
                                SQL_INLINE_TABLE_VALUED_FUNCTION,
                                SQL_SCALAR_FUNCTION,
                                SQL_STORED_PROCEDURE,
                                SQL_TABLE_VALUED_FUNCTION -> eachFile(objectTypeFile, file -> {
                                    String objectName = FilenameUtils.getBaseName(file.getName());
                                    ObjectIdentifier objectIdentifier = new ObjectIdentifier(objectType, catalog, schema, objectName);
                                    captureObject(ret, objectIdentifier, file);
                                }
                        );
                        case PRIMARY_KEY, INDEX, FOREIGN_KEY_CONSTRAINT, SQL_TRIGGER -> eachFile(objectTypeFile, tableFile -> {
                            String table = tableFile.getName();
                            ObjectIdentifier parentObjectIdentifier = new ObjectIdentifier(USER_TABLE, catalog, schema, table);
                            eachFile(tableFile, file -> {
                                String objectName = FilenameUtils.getBaseName(file.getName());
                                ObjectIdentifier objectIdentifier = new ObjectIdentifier(objectType, catalog, schema, objectName, parentObjectIdentifier);
                                captureObject(ret, objectIdentifier, file);
                            });
                        });
                    }
                });
            });
        });
        return ret;
    }

    private static void captureObject(Map<ObjectIdentifier, ObjectSignature> ret, ObjectIdentifier objectIdentifier, File objectFile) {
        try {
            String sql = FileUtils.readFileToString(objectFile, StandardCharsets.UTF_8);
            ret.put(
                    objectIdentifier,
                    new ObjectSignature(getHash(objectIdentifier.getType(), sql))
            );
        } catch (IOException e) {
            log.error("Failed to read " + objectFile, e);
        }
    }

    static ObjectSignature getObjectSignature(String type, String sql) {
        return new ObjectSignature(HashCalculator.getHash(type, sql));
    }

    static byte[] getHash(String type, String sql) {
        String cleanSql = cleanSql(sql);
        if (SQL_STORED_PROCEDURE.equals(type) ||
            SQL_INLINE_TABLE_VALUED_FUNCTION.equals(type) ||
            SQL_SCALAR_FUNCTION.equals(type) ||
            SQL_TABLE_VALUED_FUNCTION.equals(type) ||
            SQL_TRIGGER.equals(type) ||
            VIEW.equals(type)) {
            cleanSql = cleanCreateOrReplaceSql(cleanSql);
        }

        return getHash(cleanSql);
    }

    public static Map<ObjectIdentifier, ObjectSignature> captureSignatures(Database database) {
        Map<ObjectIdentifier, ObjectSignature> ret = new HashMap<>();
        database.createDatabaseIntrospector().visitModuleDefinitions(new DatabaseVisitor() {
            @Override
            public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
                ret.put(
                        objectIdentifier,
                        new ObjectSignature(getHash(objectIdentifier.getType(), definition))
                );
            }
        });
        return ret;
    }

    /**
     * Standardizes the SQL text.
     * TODO: Could do something more efficient.
     */
    public static String cleanSql(@Nonnull String sql) {
        sql = sql
                .replace("\t", " ")     // Replace the tabs with spaces
                .replace("\r\n", "\n")  // Windows
                .replace("\r", "\n")    // Mac
                .trim();                // Remove leading and trailing spaces

        // Replace consecutive spaces with one space to fix tab expansion issues
        int pos = 0;
        while (true) {
            int i = sql.indexOf("  ", pos);
            if (i == -1) break;
            sql = sql.substring(0, i + 1) + sql.substring(i + 2);
            pos = i;
        }

        return sql;
    }

    static String cleanCreateOrReplaceSql(String cleanSql) {
        if (true) {
            // Handle the simple cases
            if (StringUtils.startsWithIgnoreCase(cleanSql, "CREATE PROCEDURE ")) return cleanSql;
            if (StringUtils.startsWithIgnoreCase(cleanSql, "CREATE PROC ")) return cleanSql;
            if (StringUtils.startsWithIgnoreCase(cleanSql, "CREATE FUNCTION ")) return cleanSql;
            if (StringUtils.startsWithIgnoreCase(cleanSql, "CREATE TRIGGER ")) return cleanSql;
            if (StringUtils.startsWithIgnoreCase(cleanSql, "CREATE VIEW ")) return cleanSql;

            Matcher matcher = SPROC_PATTERN.matcher(cleanSql);
            if (!matcher.matches()) return cleanSql;

            String pre = matcher.group("pre");
            String post = matcher.group("post");
            String type = matcher.group("type");
            if ("PROC".equals(type)) type = "PROCEDURE";
            return pre + "CREATE " + type + post;
        } else {
            return cleanSql;
        }
    }

    static byte[] getHash(String sql) {
        byte[] bytes = sql.getBytes(StandardCharsets.UTF_8);
        return getMessageDigest().digest(bytes);
    }

    @SneakyThrows
    static MessageDigest getMessageDigest() {
        return MessageDigest.getInstance("SHA-256");
    }
}
