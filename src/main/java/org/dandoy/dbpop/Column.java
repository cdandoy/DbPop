package org.dandoy.dbpop;

class Column {
    private final String name;
    private final boolean identity;

    public Column(String name, boolean identity) {
        this.name = name;
        this.identity = identity;
    }

    @Override
    public String toString() {
        return identity ? name + " (identity)" : name;
    }

    public String getName() {
        return name;
    }

    public boolean isIdentity() {
        return identity;
    }
}
