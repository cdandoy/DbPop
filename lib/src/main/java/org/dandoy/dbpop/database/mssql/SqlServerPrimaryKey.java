package org.dandoy.dbpop.database.mssql;

import lombok.Getter;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.PrimaryKey;

import java.util.List;
import java.util.stream.Collectors;

import static org.dandoy.dbpop.database.mssql.SqlServerIndex.getIndexedColumns;

@Getter
public class SqlServerPrimaryKey extends PrimaryKey {
    private final String typeDesc;

    public SqlServerPrimaryKey(String name, String typeDesc, List<SqlServerIndex.SqlServerIndexColumn> columns) {
        super(name, getIndexedColumns(columns));
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
