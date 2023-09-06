package org.dandoy.dbpopd.codechanges;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpopd.code.CodeService;
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

import static org.dandoy.dbpop.database.ObjectTypes.*;
import static org.dandoy.dbpop.database.mssql.SqlServerObjectTypes.TYPE_TABLE;

@Singleton
@Slf4j
public class ApplyChangesService {
    private static final Pattern SPROC_PATTERN = Pattern.compile("(?<pre>.*)(\\bCREATE\\b\\s+(OR\\s+ALTER\\s+)?)(?<type>FUNCTION|PROC|PROCEDURE|TRIGGER|VIEW)\\b(?<post>.*)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
    private final ConfigurationService configurationService;
    private final DatabaseCacheService databaseCacheService;
    private final File codeDirectory;

    public ApplyChangesService(DatabaseCacheService databaseCacheService, ConfigurationService configurationService) {
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<ExecutionsResult.Execution> uploadFileToTarget(Database targetDatabase, Collection<ObjectIdentifier> objectIdentifiers) throws SQLException {
        Connection connection = targetDatabase.getConnection();
        try (Statement statement = connection.createStatement()) {
            return uploadFileToTarget(targetDatabase, statement, objectIdentifiers);
        }
    }

    private List<ExecutionsResult.Execution> uploadFileToTarget(Database targetDatabase, Statement statement, Collection<ObjectIdentifier> objectIdentifiers) {

        Map<String, List<ObjectIdentifier>> identifiersByCatalog = new HashMap<>();
        for (ObjectIdentifier objectIdentifier : objectIdentifiers) {
            identifiersByCatalog.computeIfAbsent(objectIdentifier.getCatalog(), s -> new ArrayList<>())
                    .add(objectIdentifier);
        }
        // Create the catalog and schemas
        identifiersByCatalog.forEach((catalog, identifiers) -> {
            targetDatabase.createCatalog(catalog);
            targetDatabase.useCatalog(catalog);
            identifiers.stream().map(ObjectIdentifier::getSchema).distinct().forEach(schema -> targetDatabase.createShema(catalog, schema));
        });
        // Create the objects
        List<ExecutionsResult.Execution> ret = new ArrayList<>();
        for (Map.Entry<String, List<ObjectIdentifier>> entry : identifiersByCatalog.entrySet()) {
            String catalog = entry.getKey();
            List<ObjectIdentifier> identifiers = entry.getValue();

            targetDatabase.useCatalog(catalog);
            // TODO: We should log the progress by catalog + type, something like "Creating 123 TABLES in AdventureWorks"
            identifiers.stream()
                    .sorted(Comparator.comparing(it -> CodeService.CODE_TYPES.indexOf(it.getType())))       // Create the tables first, ...
                    .forEach(objectIdentifier -> {
                        ExecutionsResult.Execution execution = uploadFileToTarget(targetDatabase, statement, objectIdentifier);
                        ret.add(execution);
                    });
        }
        return ret;
    }

    private ExecutionsResult.Execution uploadFileToTarget(Database database, Statement statement, ObjectIdentifier objectIdentifier) {
        log.debug("Executing {}", objectIdentifier);
        File codeDirectory = configurationService.getCodeDirectory();
        File file = DbPopdFileUtils.toFile(codeDirectory, objectIdentifier);
        try {
            if (file == null) {
                throw new RuntimeException("Failed to identify " + objectIdentifier);
            }

            String sql = IOUtils.toString(file);
            String type = objectIdentifier.getType();

            if (type.equals(USER_TABLE) || type.equals(TYPE_TABLE) || type.equals(FOREIGN_KEY_CONSTRAINT) || type.equals(INDEX) || type.equals(PRIMARY_KEY)) {
                // Attempt to transition the change, but if that is not possible (new object, cannot transition), just try to execute the statement.
                if (!transitionFileToTarget(database, statement, objectIdentifier, sql)) {
                    statement.execute(sql);
                }
            } else {
                sql = HashCalculator.cleanSql(sql);
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

    boolean transitionFileToTarget(Database database, Statement statement, ObjectIdentifier objectIdentifier, String fileSql) throws SQLException {
        String dbSql = SqlFetcher.fetchSql(database, objectIdentifier);
        if (dbSql == null) return false;
        TransitionGenerator transitionGenerator = database.getTransitionGenerator(objectIdentifier.getType());
        if (!transitionGenerator.isValid()) return false;

        Transition transition = transitionGenerator.generateTransition(objectIdentifier, dbSql, fileSql);
        for (String transitionSql : transition.getSqls()) {
            statement.execute(transitionSql);
        }
        return true;
    }

    private List<ExecutionsResult.Execution> drop(Database targetDatabase, Collection<ObjectIdentifier> objectIdentifiers) {
        return objectIdentifiers.stream()
                .sorted(Comparator.comparing(it -> -CodeService.CODE_TYPES.indexOf(it.getType())))
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
