package org.dandoy.dbpopd.code;

import lombok.Getter;
import org.dandoy.dbpop.database.ObjectIdentifier;

import java.io.File;
import java.util.Objects;

@Getter
public class Change {
    private final File file;
    private final ObjectIdentifier objectIdentifier;
    private boolean fileChanged;
    private boolean databaseChanged;
    private boolean fileDeleted;
    private boolean databaseDeleted;

    public Change(File file, ObjectIdentifier objectIdentifier) {
        this.file = file;
        this.objectIdentifier = objectIdentifier;
    }

    public boolean equals(File file, ObjectIdentifier objectIdentifier) {
        if (file != null && Objects.equals(this.file, file)) {
            return true;
        }
        if (objectIdentifier != null && Objects.equals(this.objectIdentifier, objectIdentifier)) {
            return true;
        }
        return false;
    }

    public void setDatabaseChanged() {
        this.databaseChanged = true;
        this.databaseDeleted = false;
    }

    public void setDatabaseDeleted() {
        this.databaseChanged = false;
        this.databaseDeleted = true;
    }

    public void setFileChanged() {
        this.fileChanged = true;
        this.fileDeleted = false;
    }

    public void setFileDeleted() {
        this.fileChanged = false;
        this.fileDeleted = true;
    }
}
