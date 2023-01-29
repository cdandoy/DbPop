package org.dandoy.dbpopd.code;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.DatabaseVisitor;
import org.dandoy.dbpop.database.mssql.SqlServerDatabaseIntrospector;
import org.dandoy.dbpopd.utils.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Date;
import java.util.Set;

@Slf4j
public class DbToFileVisitor implements AutoCloseable, DatabaseVisitor {
    private final DatabaseIntrospector introspector;
    private final File directory;
    private final Set<File> existingFiles;

    public DbToFileVisitor(DatabaseIntrospector introspector, File directory) {
        this.introspector = introspector;
        this.directory = directory;
        this.existingFiles = CodeUtils.getCodeFiles(directory);
    }

    @Override
    public void close() {
        FileUtils.deleteFiles(existingFiles);
    }

    @Override
    public void catalog(String catalog) {
        if (introspector instanceof SqlServerDatabaseIntrospector sqlServerIntrospector) {
            sqlServerIntrospector.visitModuleDefinitions(this, catalog);
        }
    }

    @Override
    public void moduleDefinition(String catalog, String schema, String name, String moduleType, Date modifyDate, String definition) {
        File sqlFile = FileUtils.toFile(directory, catalog, schema, moduleType, name + ".sql");
        existingFiles.remove(sqlFile);
        File dir = sqlFile.getParentFile();
        if (!dir.isDirectory() && !dir.mkdirs()) throw new RuntimeException("Failed to create " + dir);
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(sqlFile.toPath())) {
            bufferedWriter.write(definition);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (!sqlFile.setLastModified(modifyDate.getTime())) {
            log.error("Failed to setLastModified on " + sqlFile);
        }
    }
}
