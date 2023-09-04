package org.dandoy.dbpopd.deploy;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SnapshotFlywayScriptGenerator extends SnapshotScriptGenerator {
    private static final Pattern FLYWAY_PATTERN = Pattern.compile("V(\\d+)__(.*).sql");
    private final File flywayDirectory;

    public SnapshotFlywayScriptGenerator(Database database, File snapshotFile, File codeDirectory, File flywayDirectory) {
        super(database, snapshotFile, codeDirectory);
        this.flywayDirectory = flywayDirectory;
    }

    public GenerateFlywayScriptsResult generateFlywayScripts(String name) {
        List<ChangeWithSource> changeWithSources = getChangeWithSources();

        File flywayFile = getNextFlywayFile(name);
        Map<ObjectIdentifier, Boolean> transitionedObjectIdentifier = new HashMap<>();
        try {
            writeChanges(flywayFile, changeWithSources, transitionedObjectIdentifier);

            String filename = flywayFile.toString();
            if (filename.startsWith("/var/opt/dbpop/")) {
                filename = filename.substring("/var/opt/dbpop/".length());
            }
            return new GenerateFlywayScriptsResult(filename, transitionedObjectIdentifier);
        } catch (IOException e) {
            throw new RuntimeException("Failed to generate " + flywayFile, e);
        }
    }


    private File getNextFlywayFile(String name) {
        int flywayIndex = getLastFlywayIndex() + 1;
        String filename = "V%d__%s.sql".formatted(flywayIndex, name);
        return new File(flywayDirectory, filename);
    }

    private int getLastFlywayIndex() {
        if (flywayDirectory.isDirectory()) {
            String[] list = flywayDirectory.list();
            if (list != null) {
                return Arrays.stream(list)
                        .map(name -> {
                            Matcher matcher = FLYWAY_PATTERN.matcher(name);
                            if (matcher.matches()) {
                                return Integer.parseInt(matcher.group(1));
                            }
                            return null;
                        })
                        .filter(Objects::nonNull)
                        .mapToInt(Integer::intValue)
                        .max()
                        .orElse(0);
            }
        } else {
            if (!flywayDirectory.mkdirs()) {
                throw new RuntimeException("Failed to create the directory " + flywayDirectory);
            }
        }
        return 0;
    }
}
