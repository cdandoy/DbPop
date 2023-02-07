package org.dandoy.dbpop.database;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

@Getter
public class ObjectIdentifier implements Comparable<ObjectIdentifier> {
    private final String type;
    private final String catalog;
    private final String schema;
    private final String name;
    private final ObjectIdentifier parent;

    @JsonCreator
    public ObjectIdentifier(
            @JsonProperty("type") String type,
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("name") String name,
            @JsonProperty("parent") ObjectIdentifier parent
    ) {
        this.type = type;
        this.catalog = catalog;
        this.schema = schema;
        this.name = name;
        this.parent = parent;
    }

    public ObjectIdentifier(String type, String catalog, String schema, String name) {
        this(type, catalog, schema, name, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ObjectIdentifier that)) return false;
        return type.equals(that.type) && Objects.equals(catalog, that.catalog) && Objects.equals(schema, that.schema) && name.equals(that.name) && Objects.equals(parent, that.parent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, catalog, schema, name, parent);
    }

    @Override
    public int compareTo(@NotNull ObjectIdentifier that) {
        int ret = compareTo(this.type, that.type);
        if (ret == 0) {
            ret = compareTo(this.catalog, that.catalog);
            if (ret == 0) {
                ret = compareTo(this.schema, that.schema);
                if (ret == 0) {
                    ret = compareTo(this.name, that.name);
                    if (ret == 0) {
                        ret = this.parent != null && that.parent != null ? this.parent.compareTo(that.getParent()) : 0;
                    }
                }
            }
        }
        return ret;
    }

    private static int compareTo(String a, String b) {
        if (a == null) a = "";
        if (b == null) b = "";
        return a.compareTo(b);
    }
}
