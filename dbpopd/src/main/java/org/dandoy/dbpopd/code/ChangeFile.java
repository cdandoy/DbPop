package org.dandoy.dbpopd.code;

import org.dandoy.dbpop.database.ObjectIdentifier;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class ChangeFile implements AutoCloseable {
    private final File file;
    private Map<ObjectIdentifier, Boolean> identifiers;

    ChangeFile(File file) {
        this.file = file;
    }

    void objectUpdated(ObjectIdentifier objectIdentifier) {
        init();
        identifiers.put(objectIdentifier, false);
    }

    void objectDeleted(ObjectIdentifier objectIdentifier) {
        init();
        identifiers.put(objectIdentifier, true);
    }

    private void init() {
        if (identifiers == null) {
            identifiers = new HashMap<>();
            if (file.exists()) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
                    while (true) {
                        String line = reader.readLine();
                        if (line == null) break;

                        int split = line.indexOf(" ");
                        String deleted = line.substring(0, split);
                        line = line.substring(split + 1);

                        split = line.indexOf(" ");
                        String type = line.substring(0, split);
                        line = line.substring(split + 1);

                        split = line.indexOf(".");
                        String catalog = line.substring(0, split);
                        line = line.substring(split + 1);

                        split = line.indexOf(".");
                        String schema = line.substring(0, split);
                        String name = line.substring(split + 1);
                        identifiers.put(
                                new ObjectIdentifier(
                                        type, catalog, schema, name
                                ),
                                "DELETED".equals(deleted)
                        );
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read " + file, e);
                }
            }
        }
    }

    @Override
    public void close() {
        if (identifiers != null) {
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
                identifiers.keySet().stream().sorted().forEach(objectIdentifier -> {
                    Boolean deleted = identifiers.get(objectIdentifier);
                    try {
                        writer.append(deleted == Boolean.TRUE ? "DELETED" : "UPDATED")
                                .append(" ")
                                .append(objectIdentifier.getType())
                                .append(" ")
                                .append(objectIdentifier.getCatalog())
                                .append(".")
                                .append(objectIdentifier.getSchema())
                                .append(".")
                                .append(objectIdentifier.getName())
                                .append('\n')
                        ;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException("Failed to write to " + file, e);
            }
        }
    }
}
