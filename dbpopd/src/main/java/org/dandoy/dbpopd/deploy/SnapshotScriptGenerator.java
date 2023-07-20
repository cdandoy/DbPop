package org.dandoy.dbpopd.deploy;

import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpop.database.Transition;
import org.dandoy.dbpop.database.TransitionGenerator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SnapshotScriptGenerator {
    protected final Database database;
    protected final File snapshotFile;
    protected final File codeDirectory;

    public SnapshotScriptGenerator(Database database, File snapshotFile, File codeDirectory) {
        this.database = database;
        this.snapshotFile = snapshotFile;
        this.codeDirectory = codeDirectory;
    }

    List<ChangeWithSource> getChangeWithSources() {
        List<ChangeWithSource> changeWithSources = new ArrayList<>();
        new SnapshotComparator(snapshotFile, codeDirectory)
                .consumeChanges((objectIdentifier, snapshotSql, fileSql) -> changeWithSources.add(new ChangeWithSource(objectIdentifier, snapshotSql, fileSql)));
        return changeWithSources;
    }

    protected static boolean appendSql(Database database, List<String> sqls, ObjectIdentifier objectIdentifier, String fromSql, String toSql) {
        TransitionGenerator transitionGenerator = database.getTransitionGenerator(objectIdentifier.getType());
        Transition transition = transitionGenerator.generateTransition(objectIdentifier, fromSql, toSql);
        if (transition.getError() == null) {
            for (String sql : transition.getSqls()) {
                sqls.add(sql + "\nGO\n");
            }
            return true;
        } else {
            sqls.add("""
                    /*
                        ERROR: %s
                    %s
                    */
                    """.formatted(
                    transition.getError(),
                    toSql)
            );
            return false;
        }
    }

    protected List<ChangeWithSource> filterChangeWithSources(List<ChangeWithSource> in) {
        // First the drops: Do not try to drop primary keys for example if the table is dropped
        List<ChangeWithSource> drops = in.stream().filter(ChangeWithSource::isDrop).toList();
        Set<ObjectIdentifier> rootDrops = drops.stream().map(ChangeWithSource::objectIdentifier).filter(objectIdentifier -> objectIdentifier.getParent() == null).collect(Collectors.toSet());
        List<ChangeWithSource> remainingDrops = drops.stream().filter(it -> !rootDrops.contains(it.objectIdentifier().getParent())).toList();

        // Isolate the creates
        List<ChangeWithSource> creates = in.stream().filter(ChangeWithSource::isCreate).toList();

        List<ChangeWithSource> out = new ArrayList<>(remainingDrops);
        out.addAll(creates);
        return out;
    }

    protected void writeChanges(File deployFile, List<ChangeWithSource> changeWithSources, Map<ObjectIdentifier, Boolean> transitionedObjectIdentifier) throws IOException {
        List<String> sqls = generateSqls(transitionedObjectIdentifier, changeWithSources);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(deployFile), StandardCharsets.UTF_8))) {
            for (String sql : sqls) {
                writer.append(sql);
            }
        }
    }

    private List<String> generateSqls(Map<ObjectIdentifier, Boolean> transitionedObjectIdentifier, List<ChangeWithSource> changeWithSources) {
        List<String> sqls = new ArrayList<>();
        List<ChangeWithSource> filterChangeWithSources = filterChangeWithSources(changeWithSources);
        for (ChangeWithSource changeWithSource : filterChangeWithSources) {
            ObjectIdentifier objectIdentifier = changeWithSource.objectIdentifier();
            boolean succeeded = appendSql(database, sqls,
                    objectIdentifier,
                    changeWithSource.snapshotSql(),
                    changeWithSource.fileSql()
            );
            transitionedObjectIdentifier.compute(objectIdentifier, (oi, x) -> x == null ? succeeded : x && succeeded);
        }
        return sqls;
    }

    protected record ChangeWithSource(ObjectIdentifier objectIdentifier, String snapshotSql, String fileSql) {
        boolean isCreate() {return snapshotSql() == null;}

        boolean isDrop() {return fileSql() == null;}
    }
}