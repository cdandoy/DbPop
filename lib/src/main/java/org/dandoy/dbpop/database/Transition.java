package org.dandoy.dbpop.database;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class Transition {
    private final ObjectIdentifier objectIdentifier;
    private final List<String> sqls = new ArrayList<>();
    private String error;

    public Transition(ObjectIdentifier objectIdentifier) {
        this.objectIdentifier = objectIdentifier;
    }

    public void addSql(String sql) {sqls.add(sql);}

    public void setError(String warning, Object... args) {
        error = warning.formatted(args);
    }
}
