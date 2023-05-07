package org.dandoy.dbpopd.code;

import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.utils.FileUtils;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.datasets.DatasetsService;
import org.dandoy.dbpopd.populate.PopulateService;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;
import org.dandoy.dbpopd.utils.IOUtils;
import org.dandoy.dbpopd.utils.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
    private final PopulateService populateService;
    private final DatasetsService datasetsService;
    private final ChangeDetector changeDetector;

    public CodeService(ConfigurationService configurationService, PopulateService populateService, DatasetsService datasetsService, ChangeDetector changeDetector) {
        this.configurationService = configurationService;
        this.populateService = populateService;
        this.datasetsService = datasetsService;
        this.changeDetector = changeDetector;
    }

    /**
     * Walk the codeDirectory returns the files and associated ObjectIdentifier, sorted by priority of execution (tables before indexes)
     */
    public static List<Pair<File, ObjectIdentifier>> getCatalogChanges(File codeDirectory, String catalog) {
        List<Pair<File, ObjectIdentifier>> ret = new ArrayList<>();
        File catalogDir = new File(codeDirectory, catalog);

        if (catalogDir.isDirectory()) {
            DbPopdFileUtils.FileToObjectIdentifierResolver resolver = DbPopdFileUtils.createFileToObjectIdentifierResolver(codeDirectory);
            try {
                Map<Integer, List<Pair<File, ObjectIdentifier>>> filesByPriority = new TreeMap<>();
                // Collect the files by priority: tables, foreign keys, indexes, stored procedures, ...
                Files.walkFileTree(catalogDir.toPath(), new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) {
                        ObjectIdentifier objectIdentifier = resolver.getObjectIdentifier(filePath);
                        if (objectIdentifier != null) {
                            filesByPriority
                                    .computeIfAbsent(CODE_TYPES.indexOf(objectIdentifier.getType()), ArrayList::new)
                                    .add(
                                            Pair.of(
                                                    filePath.toFile(),
                                                    objectIdentifier
                                            )
                                    );
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });

                return filesByPriority.entrySet().stream().flatMap(it -> it.getValue().stream()).toList();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return ret;
    }

    /**
     * Dumps the content of the SOURCE database to the file system
     */
    public DownloadResult downloadSourceToFile() {
        try (Database database = configurationService.createSourceDatabase()) {
            return downloadToFile(database);
        }
    }

    /**
     * Dumps the content of the TARGET database to the file system
     */
    public DownloadResult downloadTargetToFile() {
        try (Database database = configurationService.createTargetDatabase()) {
            return downloadToFile(database);
        }
    }

    private DownloadResult downloadToFile(Database database) {
        File codeDirectory = configurationService.getCodeDirectory();
        FileUtils.deleteRecursively(codeDirectory);
        DatabaseIntrospector databaseIntrospector = database.createDatabaseIntrospector();
        try (DbToFileVisitor dbToFileVisitor = new DbToFileVisitor(databaseIntrospector, codeDirectory)) {
            return downloadToFile(database, dbToFileVisitor);
        }
    }

    /**
     * Dumps the content of the database to the file system
     */
    private DownloadResult downloadToFile(Database database, DbToFileVisitor visitor) {
        long t0 = System.currentTimeMillis();
        database.createDatabaseIntrospector().visit(visitor);

        // Translate USER_TABLE -> Tables
        Map<String, Integer> typeCounts1 = visitor.getTypeCounts();
        Map<String, Integer> typeCounts2 = new HashMap<>();
        for (Map.Entry<String, Integer> entry : typeCounts1.entrySet()) {
            String codeType = entry.getKey();
            String text = switch (codeType) {
                case "FOREIGN_KEY_CONSTRAINT" -> "Foreign Keys";
                case "INDEX" -> "Indexes";
                case "SQL_INLINE_TABLE_VALUED_FUNCTION", "SQL_SCALAR_FUNCTION", "SQL_STORED_PROCEDURE", "SQL_TABLE_VALUED_FUNCTION", "SQL_TRIGGER" -> "Stored Procedures";
                case "USER_TABLE" -> "Tables";
                case "VIEW" -> "Views";
                default -> codeType;
            };
            Integer i = typeCounts2.computeIfAbsent(text, s -> 0);
            typeCounts2.put(text, i + entry.getValue());
        }
        List<Pair<String, Integer>> typeCounts = typeCounts2
                .entrySet()
                .stream()
                .map(it -> Pair.of(it.getKey(), it.getValue()))
                .sorted(Comparator.comparing(Pair::left))
                .toList();
        long t1 = System.currentTimeMillis();
        return new DownloadResult(
                typeCounts,
                t1 - t0
        );
    }

    @SneakyThrows
    public UploadResult uploadFileToTarget() {
        long t0 = System.currentTimeMillis();
        return changeDetector.holdingChanges(() -> {
            try (Database targetDatabase = configurationService.createTargetDatabase()) {
                try (Connection connection = targetDatabase.getConnection()) {
                    try (Statement statement = connection.createStatement()) {
                        List<UploadResult.FileExecution> fileExecutions = new ArrayList<>();

                        File codeDirectory = configurationService.getCodeDirectory();
                        for (String catalog : targetDatabase.getCatalogs()) {

                            // USE <database>
                            targetDatabase.createCatalog(catalog);
                            statement.execute("USE " + catalog);

                            List<Pair<File, ObjectIdentifier>> changes = getCatalogChanges(codeDirectory, catalog);
                            // Create the schemas
                            changes.stream()
                                    .map(it -> it.right().getSchema())
                                    .distinct()
                                    .forEach(schema -> {
                                        try {
                                            statement.getConnection().commit();
                                            if (!"dbo".equals(schema)) {
                                                statement.execute("CREATE SCHEMA " + schema);
                                            }
                                        } catch (SQLException e) {
                                            if (!e.getSQLState().equals("S0001")) {
                                                throw new RuntimeException(e);
                                            }
                                        }
                                    });

                            // Drop the tables
                            changes.stream()
                                    .filter(it -> "USER_TABLE".equals(it.right().getType()))
                                    .map(Pair::right)
                                    .forEach(objectIdentifier -> {
                                        TableName tableName = new TableName(
                                                objectIdentifier.getCatalog(),
                                                objectIdentifier.getSchema(),
                                                objectIdentifier.getName()
                                        );
                                        try {
                                            List<ForeignKey> relatedForeignKeys = targetDatabase.getRelatedForeignKeys(tableName);
                                            for (ForeignKey foreignKey : relatedForeignKeys) {
                                                targetDatabase.dropForeignKey(foreignKey);
                                            }
                                            statement.execute("DROP TABLE IF EXISTS " + targetDatabase.quote(tableName));
                                        } catch (SQLException e) {
                                            throw new RuntimeException(e);
                                        }
                                    });

                            for (Pair<File, ObjectIdentifier> change : changes) {
                                File file = change.left();
                                ObjectIdentifier objectIdentifier = change.right();
                                UploadResult.FileExecution fileExecution = execute(statement, objectIdentifier.getType(), file);
                                fileExecutions.add(fileExecution);
                            }
                        }

                        // Reload the base dataset.
                        // We must clear the cache first because it may not know about the new objects
                        if (configurationService.getDatasetsDirectory().isDirectory()) {
                            List<String> datasets = datasetsService.getDatasets();
                            String datasetToLoad = datasets.contains("base") ? "base" : datasets.contains("static") ? "static" : !datasets.isEmpty() ? datasets.get(0) : null;
                            if (datasetToLoad != null) {
                                configurationService.clearTargetDatabaseCache();
                                populateService.populate(List.of(datasetToLoad), true);
                            }
                        }

                        long t1 = System.currentTimeMillis();
                        return new UploadResult(fileExecutions, t1 - t0);
                    } finally {
                        configurationService.clearTargetDatabaseCache();
                    }
                }
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            } finally {
                changeDetector.captureObjectSignatures();
            }
        });
    }

    private static final Pattern CREATE_PATTERN = Pattern.compile("(.*\\b)CREATE(\\s+(?:FUNCTION|PROC|PROCEDURE|TRIGGER|VIEW)\\b.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

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
                introspector.visitModuleDefinitions(this, catalog);
            }

            @Override
            public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String definition) {
                String type = objectIdentifier.getType();
                String catalog = objectIdentifier.getCatalog();
                String schema = objectIdentifier.getSchema();
                String name = objectIdentifier.getName();
                File file = DbPopdFileUtils.toFile(codeDirectory, objectIdentifier);
                long databaseTime = modifyDate.getTime();
                if (file.exists()) {
                    codeFiles.remove(file);
                    String fileContent = IOUtils.toString(file);
                    if (!fileContent.equals(definition)) {
                        long fileTime = file.lastModified();
                        entries.add(new CodeDiff.Entry(
                                new TableName(catalog, schema, name),
                                type,
                                databaseTime,
                                fileTime
                        ));
                    }
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
