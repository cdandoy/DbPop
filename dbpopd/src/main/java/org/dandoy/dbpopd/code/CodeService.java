package org.dandoy.dbpopd.code;

import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.TableName;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.utils.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Singleton
@Slf4j
public class CodeService {
    static final Set<String> CODE_TYPES = Set.of(
            "SQL_INLINE_TABLE_VALUED_FUNCTION", "SQL_SCALAR_FUNCTION", "SQL_STORED_PROCEDURE", "SQL_TABLE_VALUED_FUNCTION", "SQL_TRIGGER", "VIEW"
    );

    private final ConfigurationService configurationService;

    public CodeService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    /**
     * Dumps the content of the SOURCE database to the file system
     */
    public void downloadSourceToFile() {
        try (Database database = configurationService.createSourceDatabase()) {
            downloadToFile(database);
        }
    }

    /**
     * Dumps the content of the TARGET database to the file system
     */
    public void downloadTargetToFile() {
        try (Database database = configurationService.createSourceDatabase()) {
            downloadToFile(database);
        }
    }

    /**
     * Dumps the content of the database to the file system
     */
    private void downloadToFile(Database database) {
        DatabaseIntrospector databaseIntrospector = database.createDatabaseIntrospector();
        File codeDirectory = configurationService.getCodeDirectory();
        try (DbToFileVisitor visitor = new DbToFileVisitor(databaseIntrospector, codeDirectory)) {
            databaseIntrospector.visit(visitor);
        }
    }

    public void uploadFileToTarget() {
        try (Database targetDatabase = configurationService.createTargetDatabase()) {
            try (Connection connection = targetDatabase.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    File codeDirectory = configurationService.getCodeDirectory();
                    CodeFileInspector.inspect(codeDirectory, new CodeFileInspector.CodeFileVisitor() {
                        @Override
                        @SneakyThrows
                        public void catalog(String catalog) {
                            statement.execute("USE " + catalog);
                        }

                        @Override
                        @SneakyThrows
                        public void schema(String catalog, String schema) {
                            if (!"dbo".equals(schema)) {
                                targetDatabase.createShema(catalog, schema);
                            }
                        }

                        @Override
                        @SneakyThrows
                        public void module(String catalog, String schema, String type, String name, File sqlFile) {
                            execute(statement, sqlFile);
                        }
                    });
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Pattern CREATE_PATTERN = Pattern.compile("(.*)\\bCREATE(\\s+(?:FUNCTION|PROC|PROCEDURE|TRIGGER|VIEW)\\b.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private void execute(Statement statement, File sqlFile) throws IOException {
        try (BufferedReader bufferedReader = Files.newBufferedReader(sqlFile.toPath())) {
            String sql = bufferedReader.lines().collect(Collectors.joining("\n"));
            Matcher matcher = CREATE_PATTERN.matcher(sql);
            if (matcher.matches()) {
                String pre = matcher.group(1);
                String post = matcher.group(2);
                //noinspection SqlResolve
                String createOrAlter = pre + "CREATE OR ALTER" + post;
                try {
                    log.info("Executing {}", sqlFile);
                    statement.execute(createOrAlter);
                } catch (SQLException e) {
                    log.error(e.getMessage());
                }
            } else {
                log.warn("Could not identify " + sqlFile);
            }
        }
    }

    public CodeDiff compareSourceToFile() {
        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            return compareToFile(sourceDatabase);
        }
    }

    public CodeDiff compareTargetToFile() {
        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
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
            public void moduleMeta(int objectId, String catalog, String schema, String name, String moduleType, Date modifyDate) {
                File file = FileUtils.toFile(codeDirectory, catalog, schema, moduleType, name + ".sql");
                boolean exists = file.exists();
                entries.add(new CodeDiff.Entry(
                        new TableName(catalog, schema, name),
                        moduleType,
                        modifyDate.getTime(),
                        exists ? file.lastModified() : null
                ));
                if (exists) {
                    codeFiles.remove(file);
                }
            }
        });
        for (File codeFile : codeFiles) {
            CodeUtils.toCode(codeDirectory, codeFile, (tableName, type) -> entries.add(
                    new CodeDiff.Entry(
                            tableName,
                            type,
                            null,
                            codeFile.lastModified()
                    )
            ));
        }

        return new CodeDiff(entries);
    }
}
