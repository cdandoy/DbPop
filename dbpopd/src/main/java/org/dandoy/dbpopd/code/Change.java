package org.dandoy.dbpopd.code;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.dandoy.dbpop.database.ObjectIdentifier;

import java.io.File;
import java.util.Objects;

@Getter
@Setter
@Accessors(chain = true)
public class Change {
    private final File file;
    private final ObjectIdentifier objectIdentifier;
    private boolean fileChanged;
    private boolean databaseChanged;

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
}
