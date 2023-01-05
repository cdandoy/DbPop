package org.dandoy.dbpop.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.SneakyThrows;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VirtualFkCache {
    private final File file;
    private final List<ForeignKey> foreignKeys;
    private final ObjectWriter objectWriter;

    private VirtualFkCache(File file, List<ForeignKey> foreignKeys) {
        this.file = file;
        this.foreignKeys = new ArrayList<>(foreignKeys);
        this.objectWriter = file == null ? null : new ObjectMapper()
                .writerFor(this.foreignKeys.getClass())
                .withDefaultPrettyPrinter();
    }

    public static VirtualFkCache createVirtualFkCache() {
        return new VirtualFkCache(null, new ArrayList<>());
    }

    public static VirtualFkCache createVirtualFkCache(File file) {
        try {
            ForeignKey[] fkArray = new ForeignKey[0];
            if (file.exists()) {
                fkArray = new ObjectMapper().readValue(file, ForeignKey[].class);
            }
            return new VirtualFkCache(file, List.of(fkArray));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SneakyThrows
    private void save() {
        if (objectWriter != null) {
            objectWriter.writeValue(file, foreignKeys);
        }
    }

    public void addFK(ForeignKey foreignKey) {
        foreignKeys.add(foreignKey);
        save();
    }

    public List<ForeignKey> findByPkTable(TableName tableName) {
        return foreignKeys.stream()
                .filter(it -> tableName.equals(it.getPkTableName()))
                .toList();
    }

    public List<ForeignKey> findByFkTable(TableName tableName) {
        return foreignKeys.stream()
                .filter(it -> tableName.equals(it.getFkTableName()))
                .toList();
    }

    public ForeignKey getByPkTable(TableName tableName, String fkName) {
        return foreignKeys.stream()
                .filter(it -> tableName.equals(it.getPkTableName()) && fkName.equals(it.getName()))
                .findFirst()
                .orElse(null);
    }
}
