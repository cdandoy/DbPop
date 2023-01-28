package org.dandoy.dbpop.database.mssql;

import lombok.Getter;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.PrimaryKey;

import java.util.List;
import java.util.stream.Collectors;

@Getter
public class SqlServerPrimaryKey extends PrimaryKey {
    private final String typeDesc;

    public SqlServerPrimaryKey(String name, List<String> columns, String typeDesc) {
        super(name, columns);
        this.typeDesc = typeDesc;
    }

    @Override
    public String toDDL(Database database) {
        return "CONSTRAINT %s PRIMARY KEY %s (%s)".formatted(
                database.quote(getName()),
                typeDesc,
                getColumns().stream()
                        .map(database::quote).
                        collect(Collectors.joining(", "))
        );
    }
}
