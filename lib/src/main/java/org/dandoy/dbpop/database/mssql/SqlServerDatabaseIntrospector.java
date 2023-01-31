package org.dandoy.dbpop.database.mssql;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.*;

import java.sql.*;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class SqlServerDatabaseIntrospector implements DatabaseIntrospector {
    private final Connection connection;
    private final Database database;

    @SneakyThrows
    public SqlServerDatabaseIntrospector(Database database) {
        this.database = database;
        this.connection = database.getConnection();
    }

    @Override
    @SneakyThrows
    public void visit(DatabaseVisitor databaseVisitor) {
        for (String catalog : database.getCatalogs()) {
            databaseVisitor.catalog(catalog);
        }
    }

    @Override
    @SneakyThrows
    public void visitModuleMetas(DatabaseVisitor databaseVisitor, String catalog) {
        use(catalog);
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT o.object_id, s.name AS "schema", o.name, o.type_desc, o.modify_date
                FROM sys.schemas s
                         JOIN sys.objects o ON o.schema_id = s.schema_id
                WHERE o.is_ms_shipped = 0
                  AND o.type_desc IN ('USER_TABLE', 'INDEX', 'SQL_INLINE_TABLE_VALUED_FUNCTION', 'SQL_SCALAR_FUNCTION', 'SQL_STORED_PROCEDURE', 'SQL_TABLE_VALUED_FUNCTION', 'SQL_TRIGGER', 'VIEW')
                ORDER BY s.name, o.name
                """)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    int objectId = resultSet.getInt("object_id");
                    String schema = resultSet.getString("schema");
                    String name = resultSet.getString("name");
                    String typeDesc = resultSet.getString("type_desc");
                    Timestamp modifyDate = resultSet.getTimestamp("modify_date");
                    databaseVisitor.moduleMeta(objectId, catalog, schema, name, typeDesc, modifyDate);
                }
            }
        }
    }

    @Override
    @SneakyThrows
    public void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog) {
        Collection<Table> tables = database.getTables(catalog);
        Map<TableName, Table> tablesByName = tables.stream().collect(Collectors.toMap(Table::getTableName, Function.identity()));

        use(catalog);
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT s.name AS "schema", o.name, o.type_desc, o.modify_date, sm.definition
                FROM sys.schemas s
                         JOIN sys.objects o ON o.schema_id = s.schema_id
                         LEFT JOIN sys.sql_modules sm ON sm.object_id = o.object_id
                WHERE o.is_ms_shipped = 0
                  AND o.type_desc IN ('USER_TABLE', 'INDEX', 'SQL_INLINE_TABLE_VALUED_FUNCTION', 'SQL_SCALAR_FUNCTION', 'SQL_STORED_PROCEDURE', 'SQL_TABLE_VALUED_FUNCTION', 'SQL_TRIGGER', 'VIEW')
                ORDER BY s.name, o.name
                """)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String schema = resultSet.getString("schema");
                    String name = resultSet.getString("name");
                    String typeDesc = resultSet.getString("type_desc");
                    Date modifyDate = resultSet.getTimestamp("modify_date");
                    if (typeDesc.equals("USER_TABLE")) {
                        Table table = tablesByName.get(new TableName(catalog, schema, name));
                        databaseVisitor.moduleDefinition(catalog, schema, name, typeDesc, modifyDate, table.tableDDL(database));
                        for (ForeignKey foreignKey : table.getForeignKeys()) {
                            String fkDDL = foreignKey.toDDL(database);
                            String definition = "ALTER TABLE %s ADD %s".formatted(
                                    database.quote(table.getTableName()),
                                    fkDDL
                            );
                            databaseVisitor.moduleDefinition(catalog, schema, name + "_fk_" + foreignKey.getName(), "FOREIGN_KEY_CONSTRAINT", modifyDate, definition);
                        }
                        for (Index index : table.getIndexes()) {
                            if (!index.isPrimaryKey()) {
                                databaseVisitor.moduleDefinition(catalog, schema, name + "_idx_" + index.getName(), "INDEX", modifyDate, index.toDDL(database));
                            }
                        }
                    } else {
                        String definition = resultSet.getString("definition");
                        databaseVisitor.moduleDefinition(catalog, schema, name, typeDesc, modifyDate, definition);
                    }
                }
            }
        }
    }

    private void use(String catalog) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("USE " + catalog);
        }
    }

    @Override
    @SneakyThrows
    public void visitDependencies(DatabaseVisitor databaseVisitor, String catalog) {
        use(catalog);
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT s.name       AS "schema",
                       o.name       AS "name",
                       o.type_desc  AS "type_desc",
                       ds.name      AS "d_schema",
                       do.name      AS "d_name",
                       do.type_desc AS "d_type_desc"
                FROM sys.schemas s
                         JOIN sys.objects o ON o.schema_id = s.schema_id
                         JOIN sys.sql_dependencies sd ON sd.object_id = o.object_id
                         JOIN sys.objects do ON do.object_id = sd.referenced_major_id
                         JOIN sys.schemas ds ON ds.schema_id = do.schema_id
                WHERE o.is_ms_shipped = 0
                  AND do.is_ms_shipped = 0
                  AND o.type_desc IN ('SQL_TRIGGER', 'SQL_INLINE_TABLE_VALUED_FUNCTION', 'SQL_TABLE_VALUED_FUNCTION', 'VIEW', 'SQL_SCALAR_FUNCTION', 'SQL_STORED_PROCEDURE')
                ORDER BY s.name, o.name
                """)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String schema = resultSet.getString("schema");
                    String name = resultSet.getString("name");
                    String typeDesc = resultSet.getString("type_desc");
                    String dependentSchema = resultSet.getString("d_schema");
                    String dependentName = resultSet.getString("d_name");
                    String dependentTypeDesc = resultSet.getString("d_type_desc");
                    databaseVisitor.dependency(catalog, schema, name, typeDesc, dependentSchema, dependentName, dependentTypeDesc);
                }
            }
        }
    }
}
