package org.dandoy.dbpop.database;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class TableDependencies {

    private final Database database;
    private final Map<TableName, Boolean> dependencies = new LinkedHashMap<>();

    public TableDependencies(Database database) {
        this.database = database;
    }

    public List<TableDependency> getDependencies() {
        return dependencies.entrySet().stream()
                .map(entry -> new TableDependency(entry.getKey(), entry.getValue()))
                .toList();
    }

    private void addDependency(TableName tableName, boolean optional) {
        Boolean wasOptional = dependencies.get(tableName);
        if (wasOptional == null) {  // if it didn't exist
            dependencies.put(tableName, optional);
        } else if (wasOptional && !optional) {   // if existed, was optional but is no longer optional
            dependencies.put(tableName, true);
        }
    }

    public void findDependencies(TableName tableName) {
        Table table = database.getTable(tableName);
        for (ForeignKey foreignKey : table.foreignKeys()) {
            TableName pkTableName = foreignKey.getPkTableName();
            addDependency(pkTableName, false);
        }
        for (ForeignKey foreignKey : database.getRelatedForeignKeys(tableName)) {
            TableName fkTableName = foreignKey.getFkTableName();
            addDependency(fkTableName, true);
        }
    }

    public record TableDependency(TableName tableName, boolean optional) {}
}
