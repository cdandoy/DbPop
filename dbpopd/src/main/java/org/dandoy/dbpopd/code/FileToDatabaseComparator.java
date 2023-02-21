package org.dandoy.dbpopd.code;

import lombok.Getter;
import lombok.SneakyThrows;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.TableName;

import java.io.File;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

class FileToDatabaseComparator implements CodeFileInspector.CodeFileVisitor {
    public record ToDo(ObjectIdentifier objectIdentifier, File sqlFile) {}

    private final Database targetDatabase;
    private final Statement statement;
    private final CodeDB.TimestampInserter timestampInserter;
    @Getter
    private final List<ToDo> toDos = new ArrayList<>();
    @Getter
    private final List<TableName> modifiedTableNames = new ArrayList<>();

    public FileToDatabaseComparator(Database targetDatabase, Statement statement, CodeDB.TimestampInserter timestampInserter) {
        this.targetDatabase = targetDatabase;
        this.statement = statement;
        this.timestampInserter = timestampInserter;
    }

    @Override
    @SneakyThrows
    public void catalog(String catalog) {
        targetDatabase.createCatalog(catalog);
        statement.execute("USE " + catalog);
    }

    @Override
    @SneakyThrows
    public void schema(String catalog, String schema) {
        statement.getConnection().commit();
        if (!"dbo".equals(schema)) {
            targetDatabase.createShema(catalog, schema);
        }
    }

    @Override
    @SneakyThrows
    public void module(File sqlFile, ObjectIdentifier objectIdentifier) {
        // Only load if the source file is newer than the one we have executed
        CodeTimestamps codeTimestamps = timestampInserter.getTimestamp(objectIdentifier);
        if (isFileNewerThanRecordedTimestamp(sqlFile, codeTimestamps)) {
            toDos.add(new ToDo(objectIdentifier, sqlFile));
            if ("USER_TABLE".equals(objectIdentifier.getType()) && codeTimestamps != null) { // If it is a table that hasn't been already been loaded
                modifiedTableNames.add(new TableName(objectIdentifier.getCatalog(), objectIdentifier.getSchema(), objectIdentifier.getName()));
            }
        }
    }

    private boolean isFileNewerThanRecordedTimestamp(File sqlFile, CodeTimestamps codeTimestamps) {
        // Upload if it hasn't been executed yet
        if (codeTimestamps == null) {
            return true;
        }
        long fileTime = sqlFile.lastModified();
        long recordedTime = codeTimestamps.fileTimestamp().getTime();
        // SQL Server rounds the timestamps, so the file must be at least 100ms older than file timestamp recorded in the database
        if (fileTime - 100 > recordedTime) {
            return true;
        }
        return false;
    }
}
