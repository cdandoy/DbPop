package org.dandoy.dbpopd.code;

import org.dandoy.dbpop.database.DatabaseIntrospector;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;

import java.io.File;
import java.sql.Timestamp;
import java.util.*;

public class DbToNewerFileVisitor extends DbToFileVisitor {
    private final Map<CodeDB.TimestampObject, Timestamp> timestampMap;
    private final Set<File> existingFiles;
    private final List<ObjectIdentifier> toFetch = new ArrayList<>();

    public DbToNewerFileVisitor(DatabaseIntrospector introspector, File directory, Map<CodeDB.TimestampObject, Timestamp> timestampMap) {
        super(introspector, directory);
        this.existingFiles = CodeUtils.getCodeFiles(directory);
        this.timestampMap = timestampMap;
    }

    @Override
    public void close() {
        DbPopdFileUtils.deleteFiles(existingFiles);
    }

    protected void visitCode(String catalog) {
        introspector.visitModuleMetas(this, catalog);
        introspector.visitModuleDefinitions(this, toFetch);
        toFetch.clear();
    }

    @Override
    public void moduleMeta(ObjectIdentifier objectIdentifier, Date modifyDate) {
        String catalog = objectIdentifier.getCatalog();
        String schema = objectIdentifier.getSchema();
        String name = objectIdentifier.getName();
        String type = objectIdentifier.getType();
        if (isDbPopObject(catalog, schema, name, type)) return;
        File sqlFile = DbPopdFileUtils.toFile(directory, catalog, schema, type, name + ".sql");
        if (existingFiles.remove(sqlFile)) { // Only needed if the file exists
            if (isFileNewerThanDb(catalog, schema, name, type, modifyDate, sqlFile)) {
                return;
            }
        }
        toFetch.add(objectIdentifier);
    }

    protected boolean isFileNewerThanDb(String catalog, String schema, String name, String moduleType, Date modifyDate, File sqlFile) {
        long lastModified = sqlFile.lastModified();
        // If we have a timestampMap, use it
        Timestamp dbTimestamp = timestampMap.get(new CodeDB.TimestampObject(moduleType, catalog, schema, name));
        if (dbTimestamp != null) { // The code has been uploaded by dbpop.
            return dbTimestamp.getTime() <= lastModified;
        } else {
            // The object doesn't exist in the dbpop_timestamps table, so it must have been created by the developer.
            // We still want to check the modifyDate.
        }

        return super.isFileNewerThanDb(catalog, schema, name, moduleType, modifyDate, sqlFile);
    }

    @SuppressWarnings("unused")
    private boolean isDbPopObject(String catalog, String schema, String name, String moduleType) {
        return "dbpop_timestamps".equals(name);
    }
}
