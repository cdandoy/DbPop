package org.dandoy.dbpopd.code2;

import jakarta.inject.Singleton;
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
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("SqlSourceToSinkFlow")
@Singleton
@Slf4j
public class CodeService {
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

    public ExecutionsResult.Execution uploadFileToTarget(ObjectIdentifier objectIdentifier) {
        try (Database targetDatabase = configurationService.createTargetDatabase()) {
            Connection connection = targetDatabase.getConnection();
            try (Statement statement = connection.createStatement()) {
                String catalog = objectIdentifier.getCatalog();
                // USE <database>
                targetDatabase.createCatalog(catalog);
                statement.execute("USE " + catalog);

                // CREATE SCHEMA
                String schema = objectIdentifier.getSchema();
                targetDatabase.createShema(catalog, schema);

                ExecutionsResult.Execution execution = execute(statement, objectIdentifier);
                connection.commit();
                return execution;
            } finally {
                databaseCacheService.clearTargetDatabaseCache();
            }
        } catch (SQLException | IOException e) {
            log.error("Failed to execute", e);
            return new ExecutionsResult.Execution(objectIdentifier, e.getMessage());
        }
    }

    private static final Pattern SPROC_PATTERN = Pattern.compile("(?<pre>.*)(\\bCREATE\\b +(OR +ALTER +)?)(?<type>FUNCTION|PROC|PROCEDURE|TRIGGER|VIEW)\\b(?<post>.*)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);

    private ExecutionsResult.Execution execute(Statement statement, ObjectIdentifier objectIdentifier) throws IOException {
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
                    statement.execute(sql);
                }
            }
            return new ExecutionsResult.Execution(objectIdentifier, null);
        } catch (Exception e) {
            log.error("Failed to upload " + objectIdentifier, e);
            return new ExecutionsResult.Execution(objectIdentifier, e.getMessage());
        }
    }

    public ExecutionsResult.Execution drop(ObjectIdentifier objectIdentifier) {
        try (Database targetDatabase = configurationService.createTargetDatabase()) {
            targetDatabase.dropObject(objectIdentifier);
            return new ExecutionsResult.Execution(objectIdentifier, null);
        } catch (Exception e) {
            return new ExecutionsResult.Execution(objectIdentifier, e.getMessage());
        }
    }

    public void deleteFile(ObjectIdentifier objectIdentifier) {
        File file = DbPopdFileUtils.toFile(codeDirectory, objectIdentifier);
        if (file == null || !file.isFile()) throw new RuntimeException("Invalid file " + file);
        if (!file.delete() && file.exists()) {
            throw new RuntimeException("Failed to delete " + file);
        }
    }
}
