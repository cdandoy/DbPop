package org.dandoy.dbpop.upload;

import java.util.Collection;

public class Dataset {
    private final String name;
    private final Collection<DataFile> dataFiles;

    public Dataset(String name, Collection<DataFile> dataFiles) {
        this.name = name;
        this.dataFiles = dataFiles;
    }

    public String getName() {
        return name;
    }

    public Collection<DataFile> getDataFiles() {
        return dataFiles;
    }
}
