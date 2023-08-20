package org.dandoy.dbpopd.code2;

import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.DatabaseCacheService;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;
import org.dandoy.dbpopd.utils.IOUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings("SqlSourceToSinkFlow")
@Singleton
@Slf4j
public class CodeService {
    public static final List<String> CODE_TYPES = List.of(
            "USER_TABLE", "PRIMARY_KEY", "INDEX", "FOREIGN_KEY_CONSTRAINT", "SQL_INLINE_TABLE_VALUED_FUNCTION", "SQL_SCALAR_FUNCTION", "SQL_STORED_PROCEDURE", "SQL_TABLE_VALUED_FUNCTION", "SQL_TRIGGER", "VIEW"
    );
    private final ConfigurationService configurationService;
    private final DatabaseCacheService databaseCacheService;
    private final File codeDirectory;

    public CodeService(DatabaseCacheService databaseCacheService, ConfigurationService configurationService) {
        this.databaseCacheService = databaseCacheService;
        this.codeDirectory = configurationService.getCodeDirectory();
        this.configurationService = configurationService;
    }

    public void download(ObjectIdentifier objectIdentifier) {
        try (Database database = configurationService.createTargetDatabase()) {
            database.createDatabaseIntrospector().visitModuleDefinitions(List.of(objectIdentifier), new DatabaseVisitor() {
                @Override
                public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
                    File file = DbPopdFileUtils.toFile(codeDirectory, objectIdentifier);
                    if (!file.getParentFile().mkdirs() && !file.getParentFile().isDirectory()) throw new RuntimeException("Failed to create the directory " + file.getParent());
                    try (Writer writer = new OutputStreamWriter(new FileOutputStream(file))) {
                        writer.write(definition);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    @SneakyThrows
    List<ExecutionsResult.Execution> applyFiles(Collection<ObjectIdentifier> uploads, Collection<ObjectIdentifier> drops) {
        try (Database targetDatabase = configurationService.createTargetDatabase()) {

            List<ExecutionsResult.Execution> uploadExecutions = uploadFileToTarget(targetDatabase, uploads);
            List<ExecutionsResult.Execution> dropExecutions = drop(targetDatabase, drops);

            targetDatabase.getConnection().commit();
            databaseCacheService.clearTargetDatabaseCache();

            ArrayList<ExecutionsResult.Execution> ret = new ArrayList<>();
            ret.addAll(uploadExecutions);
            ret.addAll(dropExecutions);
            return ret;
        }
    }

    private List<ExecutionsResult.Execution> uploadFileToTarget(Database targetDatabase, Collection<ObjectIdentifier> objectIdentifiers) throws SQLException {
        Connection connection = targetDatabase.getConnection();
        try (Statement statement = connection.createStatement()) {
            return uploadFileToTarget(targetDatabase, statement, objectIdentifiers);
        }
    }

    private List<ExecutionsResult.Execution> uploadFileToTarget(Database targetDatabase, Statement statement, Collection<ObjectIdentifier> objectIdentifiers) {
        return objectIdentifiers.stream()
                // Group and execute by catalog
                .collect(Collectors.groupingBy(ObjectIdentifier::getCatalog))
                .entrySet()
                .stream()
                .map(entry -> {
                    String catalog = entry.getKey();
                    List<ObjectIdentifier> identifiers = entry.getValue();

                    // Create and use the catalog
                    targetDatabase.createCatalog(catalog);
                    try {
                        statement.execute("USE " + catalog);
                    } catch (SQLException e) {
                        throw new RuntimeException("Failed to USE " + catalog);
                    }

                    // Create the schemas
                    identifiers.stream().map(ObjectIdentifier::getSchema).distinct().forEach(schema -> targetDatabase.createShema(catalog, schema));

                    return identifiers.stream()
                            .sorted(Comparator.comparing(it -> CODE_TYPES.indexOf(it.getType())))
                            .map(objectIdentifier -> uploadFileToTarget(statement, objectIdentifier))
                            .toList();
                })
                .flatMap(Collection::stream)
                .toList();
    }

    private static final Pattern SPROC_PATTERN = Pattern.compile("(?<pre>.*)(\\bCREATE\\b\\s+(OR\\s+ALTER\\s+)?)(?<type>FUNCTION|PROC|PROCEDURE|TRIGGER|VIEW)\\b(?<post>.*)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);

    private ExecutionsResult.Execution uploadFileToTarget(Statement statement, ObjectIdentifier objectIdentifier) {
        log.debug("Executing {}", objectIdentifier);
        File codeDirectory = configurationService.getCodeDirectory();
        File file = DbPopdFileUtils.toFile(codeDirectory, objectIdentifier);
        try {
            if (file == null) {
                throw new RuntimeException("Failed to identify " + objectIdentifier);
            }

            String sql = IOUtils.toString(file);
            sql = HashCalculator.cleanSql(sql);

            String type = objectIdentifier.getType();

            if (type.equals("USER_TABLE") || type.equals("FOREIGN_KEY_CONSTRAINT") || type.equals("INDEX") || type.equals("PRIMARY_KEY")) {
                statement.execute(sql);
            } else {
                Matcher matcher = SPROC_PATTERN.matcher(sql);
                if (matcher.matches()) {
                    String pre = matcher.group("pre");
                    String post = matcher.group("post");
                    String type2 = matcher.group("type");
                    //noinspection SqlResolve
                    sql = pre + "CREATE OR ALTER " + type2 + post;
                } else {
                    log.warn("SPROC_PATTERN missed");
                }
                statement.execute(sql);
            }
            return new ExecutionsResult.Execution(objectIdentifier, null);
        } catch (Exception e) {
            log.error("Failed to upload {} - {}", objectIdentifier, file);
            log.error("     {}", e.getMessage());
            return new ExecutionsResult.Execution(objectIdentifier, e.getMessage());
        }
    }

    private List<ExecutionsResult.Execution> drop(Database targetDatabase, Collection<ObjectIdentifier> objectIdentifiers) {
        return objectIdentifiers.stream()
                .sorted(Comparator.comparing(it -> -CODE_TYPES.indexOf(it.getType())))
                .map(objectIdentifier -> {
                    try {
                        targetDatabase.dropObject(objectIdentifier);
                        return new ExecutionsResult.Execution(objectIdentifier, null);
                    } catch (Exception e) {
                        log.error("Failed to drop " + objectIdentifiers, e);
                        return new ExecutionsResult.Execution(objectIdentifier, e.getMessage());
                    }
                })
                .toList();
    }

    public void deleteFile(ObjectIdentifier objectIdentifier) {
        File file = DbPopdFileUtils.toFile(codeDirectory, objectIdentifier);
        if (file == null || !file.isFile()) throw new RuntimeException("Invalid file " + file);
        if (!file.delete() && file.exists()) {
            throw new RuntimeException("Failed to delete " + file);
        }
    }
}
