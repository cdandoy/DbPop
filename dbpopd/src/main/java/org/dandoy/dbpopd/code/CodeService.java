package org.dandoy.dbpopd.code;

import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.utils.FileUtils;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.datasets.DatasetsService;
import org.dandoy.dbpopd.populate.PopulateService;
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
    private final PopulateService populateService;
    private final DatasetsService datasetsService;

    public CodeService(ConfigurationService configurationService, PopulateService populateService, DatasetsService datasetsService) {
        this.configurationService = configurationService;
        this.populateService = populateService;
        this.datasetsService = datasetsService;
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
            try (CodeDB.TimestampInserter timestampInserter = CodeDB.createTimestampInserter(database)) {
                DatabaseIntrospector databaseIntrospector = database.createDatabaseIntrospector();
                try (TargetDbToFileVisitor targetDbToFileVisitor = new TargetDbToFileVisitor(timestampInserter, databaseIntrospector, codeDirectory)) {
                    return downloadToFile(database, targetDbToFileVisitor);
                }
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

    @SneakyThrows
    public UploadResult uploadFileToTarget() {
        long t0 = System.currentTimeMillis();
        try (Database targetDatabase = configurationService.createTargetDatabase()) {
            try (Connection connection = targetDatabase.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    try (CodeDB.TimestampInserter timestampInserter = CodeDB.createTimestampInserter(targetDatabase)) {
                        File codeDirectory = configurationService.getCodeDirectory();
                        FileToDatabaseComparator comparator = new FileToDatabaseComparator(targetDatabase, statement, timestampInserter);
                        CodeFileInspector.inspect(codeDirectory, comparator);

                        try {
                            List<TableName> modifiedTableNames = comparator.getModifiedTableNames();
                            List<ForeignKey> foreignKeys = modifiedTableNames.stream()
                                    .map(targetDatabase::getRelatedForeignKeys)
                                    .flatMap(Collection::stream)
                                    .collect(Collectors.toList());
                            // drop the FKs that point to the modified tables
                            for (ForeignKey foreignKey : foreignKeys) {
                                targetDatabase.dropForeignKey(foreignKey);
                            }

                            // drop the modified tables
                            for (TableName modifiedTableName : modifiedTableNames) {
                                statement.execute("DROP TABLE " + targetDatabase.quote(modifiedTableName));
                            }

                            // Execute the updates
                            List<FileToDatabaseComparator.ToDo> toDos = comparator.getToDos();
                            List<UploadResult.FileExecution> fileExecutions = new ArrayList<>();
                            for (FileToDatabaseComparator.ToDo toDo : toDos) {
                                ObjectIdentifier objectIdentifier = toDo.objectIdentifier();
                                File sqlFile = toDo.sqlFile();
                                UploadResult.FileExecution fileExecution = execute(statement, objectIdentifier.getType(), sqlFile);
                                fileExecutions.add(fileExecution);
                                timestampInserter.addTimestamp(objectIdentifier, new Timestamp(sqlFile.lastModified()));

                                // Don't re-create later the foreign key that we dropped if it is re-created here
                                if ("FOREIGN_KEY_CONSTRAINT".equals(toDo.objectIdentifier().getType())) {
                                    foreignKeys.removeIf(foreignKey -> foreignKey.getName().equals(objectIdentifier.getName()) &&
                                                                       foreignKey.getPkTableName().getSchema().equals(objectIdentifier.getSchema()) &&
                                                                       foreignKey.getPkTableName().getCatalog().equals(objectIdentifier.getCatalog())
                                    );
                                }
                            }

                            // Reload the base dataset. If we have dropped customers, we cannot re-enable the invoices_customers_fk without reloading the data
                            // We must clear the cache first because it may not know about the new objects
                            if (configurationService.getDatasetsDirectory().isDirectory()) {
                                List<String> datasets = datasetsService.getDatasets();
                                String datasetToLoad = datasets.contains("base") ? "base" : datasets.contains("static") ? "static" : !datasets.isEmpty() ? datasets.get(0) : null;
                                if (datasetToLoad != null) {
                                    configurationService.clearTargetDatabaseCache();
                                    populateService.populate(List.of(datasetToLoad), true);
                                }
                            }

                            // Recreate the dropped FKs that haven't been re-created
                            for (ForeignKey foreignKey : foreignKeys) {
                                targetDatabase.createForeignKey(foreignKey);
                            }

                            long t1 = System.currentTimeMillis();
                            return new UploadResult(fileExecutions, t1 - t0);
                        } finally {
                            configurationService.clearTargetDatabaseCache();
                        }
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
