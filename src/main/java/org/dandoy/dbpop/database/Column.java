package org.dandoy.dbpop.database;

class Column {
    private final String name;
    private final boolean identity;
    private final boolean binary;

    public Column(String name, boolean identity, boolean binary) {
        this.name = name;
        this.identity = identity;
        this.binary = binary;
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

    public boolean isBinary() {
        return binary;
    }
}
