package org.dandoy.dbpopd.code;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * The visitor that downloads the code from the target database and dumps it into files.
 * The difference with DbToFileVisitor is that we do not want to download code that has not been modified.
 */
@Slf4j
public class TargetDbToFileVisitor extends DbToFileVisitor {
    private final Set<File> existingFiles;
    private final List<ObjectIdentifier> toFetch = new ArrayList<>();
    private final CodeDB.TimestampInserter timestampInserter;

    public TargetDbToFileVisitor(CodeDB.TimestampInserter timestampInserter, DatabaseIntrospector databaseIntrospector, File codeDirectory) {
        super(databaseIntrospector, codeDirectory);
        this.timestampInserter = timestampInserter;
        this.existingFiles = CodeUtils.getCodeFiles(directory);
    }

    @SneakyThrows
    @Override
    public void close() {
        DbPopdFileUtils.deleteFiles(existingFiles);
    }

    protected void visitCode(String catalog) {
        introspector.visitModuleMetas(this, catalog);
        introspector.visitModuleDefinitions(this, toFetch);
        toFetch.clear();
    }

    @SneakyThrows
    @Override
    public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, String definition) {
        super.moduleDefinition(objectIdentifier, modifyDate, definition);
        if (definition != null) {
            timestampInserter.addTimestamp(objectIdentifier, new Timestamp(modifyDate.getTime()));
        }
    }

    @Override
    public void moduleMeta(ObjectIdentifier objectIdentifier, Date modifyDate) {
        String catalog = objectIdentifier.getCatalog();
        String schema = objectIdentifier.getSchema();
        String name = objectIdentifier.getName();
        String type = objectIdentifier.getType();
        if (isDbPopObject(catalog, schema, name, type)) return;
        File sqlFile = DbPopdFileUtils.toFile(directory, catalog, schema, type, name + ".sql");

        existingFiles.remove(sqlFile);

        if (mustDownload(objectIdentifier, modifyDate, sqlFile)) {
            toFetch.add(objectIdentifier);
        }
    }

    /**
     * We do not want to download the code from the database to the .sql file
     * o if the object has not been updated since it was uploaded
     * o if the file is newer than the uploaded timestamp
     */
    protected boolean mustDownload(ObjectIdentifier objectIdentifier, Date modifyDate, File sqlFile) {
        long fileTime = sqlFile.lastModified();

        // File doesn't exist
        if (fileTime == 0) return true;

        CodeTimestamps codeTimestamps = timestampInserter.getTimestamp(objectIdentifier);
        if (codeTimestamps != null) { // The code has been uploaded by dbpop.
            // We have captured the execution time from the OS, the container may be slightly off
            if (modifyDate.getTime() - codeTimestamps.codeTimestamp().getTime() < 1000) {
                // Code has not been updated since it was uploaded
                return false;
            }
            if (fileTime > codeTimestamps.fileTimestamp().getTime()) {
                // Both the file and the database have been modified
                log.warn("Merge conflict: {}", objectIdentifier);
                return true;
            }
            return true;
        } else {
            // The object doesn't exist in the dbpop_timestamps table, so it must have been created by the developer.
            // We still want to check the modifyDate.
            if (modifyDate.getTime() <= sqlFile.lastModified()) {
                log.warn("Merge conflict: {}", objectIdentifier);
            }
            return true;
        }
    }

    @SuppressWarnings("unused")
    private boolean isDbPopObject(String catalog, String schema, String name, String moduleType) {
        return "dbpop_timestamps".equals(name);
    }
}
