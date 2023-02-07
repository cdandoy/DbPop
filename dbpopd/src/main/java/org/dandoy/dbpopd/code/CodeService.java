package org.dandoy.dbpopd.code;

import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.utils.FileUtils;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;
import org.dandoy.dbpopd.utils.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Singleton
@Slf4j
public class CodeService {
    static final List<String> CODE_TYPES = List.of(
            "USER_TABLE", "FOREIGN_KEY_CONSTRAINT", "INDEX", "SQL_INLINE_TABLE_VALUED_FUNCTION", "SQL_SCALAR_FUNCTION", "SQL_STORED_PROCEDURE", "SQL_TABLE_VALUED_FUNCTION", "SQL_TRIGGER", "VIEW"
    );

    private final ConfigurationService configurationService;

    public CodeService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    /**
     * Dumps the content of the SOURCE database to the file system
     */
    public DownloadResult downloadSourceToFile() {
        try (Database database = configurationService.createSourceDatabase()) {
            File codeDirectory = configurationService.getCodeDirectory();
            FileUtils.deleteRecursively(codeDirectory);
            DatabaseIntrospector databaseIntrospector = database.createDatabaseIntrospector();
            try (DbToFileVisitor dbToFileVisitor = new DbToFileVisitor(databaseIntrospector, codeDirectory)) {
                return downloadToFile(database, dbToFileVisitor);
            }
        }
    }

    /**
     * Dumps the content of the TARGET database to the file system
     */
    public DownloadResult downloadTargetToFile() {
        try (Database database = configurationService.createTargetDatabase()) {
            File codeDirectory = configurationService.getCodeDirectory();
            Map<CodeDB.TimestampObject, Timestamp> timestampMap = CodeDB.getObjectTimestampMap(database.getConnection());
            DatabaseIntrospector databaseIntrospector = database.createDatabaseIntrospector();
            try (DbToNewerFileVisitor dbToNewerFileVisitor = new DbToNewerFileVisitor(databaseIntrospector, codeDirectory, timestampMap)) {
                return downloadToFile(database, dbToNewerFileVisitor);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Dumps the content of the database to the file system
     */
    private DownloadResult downloadToFile(Database database, DbToFileVisitor visitor) {
        long t0 = System.currentTimeMillis();
        database.createDatabaseIntrospector().visit(visitor);

        // Translate USER_TABLE -> Tables
        List<Pair<String, Integer>> typeCounts = visitor
                .getTypeCounts()
                .entrySet().stream()
                .map(it -> {
                    String codeType = it.getKey();
                    return Pair.of(
                            switch (codeType) {
                                case "FOREIGN_KEY_CONSTRAINT" -> "Foreign Keys";
                                case "INDEX" -> "Indexes";
                                case "SQL_INLINE_TABLE_VALUED_FUNCTION", "SQL_SCALAR_FUNCTION", "SQL_STORED_PROCEDURE", "SQL_TABLE_VALUED_FUNCTION", "SQL_TRIGGER" -> "Stored Procedures";
                                case "USER_TABLE" -> "Tables";
                                case "VIEW" -> "Views";
                                default -> codeType;
                            },
                            it.getValue());
                })
                .sorted(Comparator.comparing(Pair::left))
                .toList();
        long t1 = System.currentTimeMillis();
        return new DownloadResult(
                typeCounts,
                t1 - t0
        );
    }

    public UploadResult uploadFileToTarget() {
        try (Database targetDatabase = configurationService.createTargetDatabase()) {
            try (Connection connection = targetDatabase.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    try (CodeDB.TimestampInserter timestampInserter = CodeDB.createTimestampInserter(targetDatabase)) {
                        List<UploadResult.FileExecution> fileExecutions = new ArrayList<>();
                        long t0 = System.currentTimeMillis();
                        File codeDirectory = configurationService.getCodeDirectory();
                        CodeFileInspector.inspect(codeDirectory, new CodeFileInspector.CodeFileVisitor() {
                            @Override
                            @SneakyThrows
                            public void catalog(String catalog) {
                                targetDatabase.createCatalog(catalog);
                                statement.execute("USE " + catalog);
                            }

                            @Override
                            @SneakyThrows
                            public void schema(String catalog, String schema) {
                                statement.getConnection().commit();
                                if (!"dbo".equals(schema)) {
                                    targetDatabase.createShema(catalog, schema);
                                }
                            }

                            @Override
                            @SneakyThrows
                            public void module(String catalog, String schema, String type, String name, File sqlFile) {
                                UploadResult.FileExecution fileExecution = execute(statement, type, sqlFile);
                                fileExecutions.add(fileExecution);
                                if (name.endsWith(".sql")) {
                                    String objectName = name.substring(0, name.length() - 4);
                                    timestampInserter.addTimestamp(type, catalog, schema, objectName, new Timestamp(sqlFile.lastModified()));
                                }
                            }
                        });
                        long t1 = System.currentTimeMillis();
                        return new UploadResult(fileExecutions, t1 - t0);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Pattern CREATE_PATTERN = Pattern.compile("(.*)\\bCREATE(\\s+(?:FUNCTION|PROC|PROCEDURE|TRIGGER|VIEW)\\b.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private UploadResult.FileExecution execute(Statement statement, String type, File sqlFile) throws IOException {
        log.info("Executing {}", sqlFile);

        String sqlFileName = sqlFile.getName();
        String objectName = sqlFileName.endsWith(".sql") ? sqlFileName.substring(0, sqlFileName.length() - 4) : null;

        try (BufferedReader bufferedReader = Files.newBufferedReader(sqlFile.toPath())) {
            String sql = bufferedReader.lines().collect(Collectors.joining("\n"));
            if (type.equals("USER_TABLE") || type.equals("FOREIGN_KEY_CONSTRAINT") || type.equals("INDEX")) {
                statement.execute(sql);
            } else {
                Matcher matcher = CREATE_PATTERN.matcher(sql);
                if (matcher.matches()) {
                    String pre = matcher.group(1);
                    String post = matcher.group(2);
                    //noinspection SqlResolve
                    String createOrAlter = pre + "CREATE OR ALTER" + post;
                    statement.execute(createOrAlter);
                } else {
                    log.warn("Could not identify " + sqlFile);
                }
            }
            return new UploadResult.FileExecution(sqlFile.getPath(), type, objectName, null);
        } catch (SQLException e) {
            log.error(e.getMessage());
            return new UploadResult.FileExecution(sqlFile.getPath(), type, objectName, e.getMessage());
        }
    }

    public CodeDiff compareSourceToFile() {
        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            return compareToFile(sourceDatabase);
        }
    }

    public CodeDiff compareTargetToFile() {
        try (Database sourceDatabase = configurationService.createTargetDatabase()) {
            return compareToFile(sourceDatabase);
        }
    }

    private CodeDiff compareToFile(Database database) {
        List<CodeDiff.Entry> entries = new ArrayList<>();
        File codeDirectory = configurationService.getCodeDirectory();
        Set<File> codeFiles = CodeUtils.getCodeFiles(codeDirectory);
        DatabaseIntrospector introspector = database.createDatabaseIntrospector();
        introspector.visit(new DatabaseVisitor() {
            @Override
            public void catalog(String catalog) {
                if (catalog.equals("tempdb")) return;
                introspector.visitModuleMetas(this, catalog);
            }

            @Override
            public void moduleMeta(ObjectIdentifier objectIdentifier, Date modifyDate) {
                String type = objectIdentifier.getType();
                String catalog = objectIdentifier.getCatalog();
                String schema = objectIdentifier.getSchema();
                String name = objectIdentifier.getName();
                File file = DbPopdFileUtils.toFile(codeDirectory, catalog, schema, type, name + ".sql");
                boolean exists = file.exists();
                long databaseTime = modifyDate.getTime();
                if (exists) {
                    long fileTime = file.lastModified();
                    if (databaseTime != fileTime) {
                        entries.add(new CodeDiff.Entry(
                                new TableName(catalog, schema, name),
                                type,
                                databaseTime,
                                fileTime
                        ));
                    }
                    codeFiles.remove(file);
                } else {
                    entries.add(new CodeDiff.Entry(
                            new TableName(catalog, schema, name),
                            type,
                            databaseTime,
                            null
                    ));
                }
            }
        });
        for (File codeFile : codeFiles) {
            CodeUtils.toCode(codeDirectory, codeFile, (tableName, type) -> {
                if ("FOREIGN_KEY_CONSTRAINT".equals(type)) return; // FKs are reported as part of the table
                entries.add(
                        new CodeDiff.Entry(
                                tableName,
                                type,
                                null,
                                codeFile.lastModified()
                        )
                );
            });
        }
        entries.sort(Comparator.comparing(CodeDiff.Entry::tableName));

        return new CodeDiff(entries);
    }
}
