package org.dandoy.dbpopd.code;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpopd.ConfigurationService;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
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
     * Dumps the content of the source database to the file system
     */
    public void sourceToFile() {
        try (Database sourceDatabase = configurationService.createSourceDatabase()) {
            DatabaseIntrospector databaseIntrospector = sourceDatabase.createDatabaseIntrospector();
            File codeDirectory = configurationService.getCodeDirectory();
            try (DbToFileVisitor visitor = new DbToFileVisitor(databaseIntrospector, codeDirectory)) {
                databaseIntrospector.visit(visitor);
            }
        }
    }

    public void fileToTarget() {
        try (Database targetDatabase = configurationService.createTargetDatabase()) {
            try (Connection connection = targetDatabase.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    File codeDirectory = configurationService.getCodeDirectory();
                    File[] catalogDirs = codeDirectory.listFiles();
                    if (catalogDirs != null) {
                        Set<String> catalogs = new HashSet<>();
                        for (File catalogDir : catalogDirs) {
                            String catalog = catalogDir.getName();
                            if (catalogs.add(catalog)) {
                                targetDatabase.createCatalog(catalog);
                                statement.execute("USE " + catalog);
                            }
                            Set<String> schemas = new HashSet<>();
                            File[] schemaFiles = catalogDir.listFiles();
                            if (schemaFiles != null) {
                                for (File schemaFile : schemaFiles) {
                                    String schema = schemaFile.getName();
                                    if (schemas.add(schema)) {
                                        if (!"dbo".equals(schema)) {
                                            targetDatabase.createShema(catalog, schema);
                                        }
                                    }
                                    File[] codeTypeFiles = schemaFile.listFiles();
                                    if (codeTypeFiles != null) {
                                        for (File codeTypeFile : codeTypeFiles) {
                                            String codeType = codeTypeFile.getName();
                                            if (CODE_TYPES.contains(codeType)) {
                                                File[] sqlFiles = codeTypeFile.listFiles();
                                                if (sqlFiles != null) {
                                                    for (File sqlFile : sqlFiles) {
                                                        if (sqlFile.isFile() && sqlFile.getName().endsWith(".sql")) {
                                                            execute(statement, sqlFile);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Pattern CREATE_PATTERN = Pattern.compile("(.*)\\bCREATE(\\s+(?:FUNCTION|PROC|PROCEDURE|TRIGGER|VIEW)\\b.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private void execute(Statement statement, File sqlFile) throws IOException, SQLException {
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
}
