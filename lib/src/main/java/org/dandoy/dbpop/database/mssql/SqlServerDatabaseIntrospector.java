package org.dandoy.dbpop.database.mssql;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.utils.StringUtils;

import java.sql.*;
import java.util.Date;
import java.util.*;
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
    public void visitModuleDefinitions(DatabaseVisitor databaseVisitor, Timestamp since) {
        try {
            for (String catalog : database.getCatalogs()) {
                use(catalog);
                try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT object_id FROM sys.objects WHERE modify_date > ?")) {
                    if (database.isSqlServer()) { // SQL Server timestamps are approximate
                        since = new Timestamp(since.getTime() + 1000);
                    }
                    preparedStatement.setObject(1, since);
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        List<Integer> objectIds = new ArrayList<>();
                        while (resultSet.next()) {
                            int objectId = resultSet.getInt(1);
                            objectIds.add(objectId);
                        }
                        if (!objectIds.isEmpty()) {
                            visitModuleDefinitions(databaseVisitor, catalog, objectIds);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SneakyThrows
    public void visitModuleMetas(DatabaseVisitor databaseVisitor, String catalog) {
        use(catalog);
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT o.object_id, s.name AS "schema", o.name, o.type_desc, o.modify_date AS modify_date
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
                    databaseVisitor.moduleMeta(new SqlServerObjectIdentifier(objectId, typeDesc, catalog, schema, name), modifyDate);
                }
            }
        }
    }

    @Override
    @SneakyThrows
    public void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog) {
        use(catalog);
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT o.object_id, s.name AS "schema", o.name, o.type_desc, o.modify_date AS modify_date, sm.definition
                FROM sys.schemas s
                         JOIN sys.objects o ON o.schema_id = s.schema_id
                         LEFT JOIN sys.sql_modules sm ON sm.object_id = o.object_id
                WHERE o.is_ms_shipped = 0
                  AND o.type_desc IN ('USER_TABLE', 'INDEX', 'SQL_INLINE_TABLE_VALUED_FUNCTION', 'SQL_SCALAR_FUNCTION', 'SQL_STORED_PROCEDURE', 'SQL_TABLE_VALUED_FUNCTION', 'SQL_TRIGGER', 'VIEW')
                ORDER BY s.name, o.name
                """)) {
            visitModuleDefinitions(databaseVisitor, catalog, preparedStatement);
        }
    }

    public void visitModuleDefinitions(DatabaseVisitor databaseVisitor, Collection<ObjectIdentifier> objectIdentifiers) {
        objectIdentifiers.stream()
                .collect(Collectors.groupingBy(ObjectIdentifier::getCatalog))
                .forEach((catalog, catalogObjectIdentifier) -> visitModuleDefinitions(databaseVisitor, catalog, catalogObjectIdentifier));
    }

    private void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog, Collection<ObjectIdentifier> objectIdentifiers) {
        List<Integer> objectIds = new ArrayList<>();
        for (ObjectIdentifier objectIdentifier : objectIdentifiers) {
            if (!(objectIdentifier instanceof SqlServerObjectIdentifier sqlServerObjectIdentifier)) throw new RuntimeException("Expected SqlServerObjectIdentifiers, found " + (objectIdentifier == null ? "null" : objectIdentifier.getClass()));
            objectIds.add(sqlServerObjectIdentifier.getObjectId());
        }
        visitModuleDefinitions(databaseVisitor, catalog, objectIds);
    }

    private void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog, List<Integer> objectIds) {
        int bindCount = 1000;
        while (!objectIds.isEmpty()) {
            int max = Math.min(objectIds.size(), bindCount);
            visitModuleDefinitions(databaseVisitor, catalog, objectIds.subList(0, max), bindCount);
            objectIds = objectIds.subList(max, objectIds.size());
        }
    }

    @SneakyThrows
    private void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog, List<Integer> objectIds, int bindCount) {

        use(catalog);
        String sql = """
                SELECT o.object_id, s.name AS "schema", o.name, o.type_desc, o.modify_date as modify_date, sm.definition
                FROM sys.schemas s
                         JOIN sys.objects o ON o.schema_id = s.schema_id
                         LEFT JOIN sys.sql_modules sm ON sm.object_id = o.object_id
                WHERE o.object_id in (%s)
                ORDER BY s.name, o.name
                """.formatted(StringUtils.repeat("?", bindCount, ","));
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            int i;
            for (i = 0; i < bindCount && i < objectIds.size(); i++) {
                preparedStatement.setInt(i + 1, objectIds.get(i));
            }
            for (; i < bindCount; i++) {
                preparedStatement.setNull(i + 1, Types.INTEGER);
            }
            visitModuleDefinitions(databaseVisitor, catalog, preparedStatement);
        }
    }

    private void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog, PreparedStatement preparedStatement) throws SQLException {
        Map<TableName, Table> tablesByName = null;
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                int objectId = resultSet.getInt("object_id");
                String schema = resultSet.getString("schema");
                String name = resultSet.getString("name");
                String typeDesc = resultSet.getString("type_desc");
                Date modifyDate = resultSet.getTimestamp("modify_date");
                SqlServerObjectIdentifier objectIdentifier = new SqlServerObjectIdentifier(objectId, typeDesc, catalog, schema, name);
                if (typeDesc.equals("USER_TABLE")) {
                    if (tablesByName == null) {
                        Collection<Table> tables = database.getTables(catalog);
                        tablesByName = tables.stream().collect(Collectors.toMap(Table::getTableName, Function.identity()));
                    }
                    Table table = tablesByName.get(new TableName(catalog, schema, name));
                    databaseVisitor.moduleDefinition(objectIdentifier, modifyDate, table.tableDDL(database));
                    for (ForeignKey foreignKey : table.getForeignKeys()) {
                        String fkDDL = foreignKey.toDDL(database);
                        String definition = "ALTER TABLE %s ADD %s".formatted(
                                database.quote(table.getTableName()),
                                fkDDL
                        );
                        databaseVisitor.moduleDefinition(new SqlServerObjectIdentifier(objectId, "FOREIGN_KEY_CONSTRAINT", catalog, schema, foreignKey.getName(), objectIdentifier), modifyDate, definition);
                    }
                    for (Index index : table.getIndexes()) {
                        if (!index.isPrimaryKey()) {
                            databaseVisitor.moduleDefinition(new SqlServerObjectIdentifier(objectId, "INDEX", catalog, schema, index.getName(), objectIdentifier), modifyDate, index.toDDL(database));
                        }
                    }
                } else {
                    String definition = resultSet.getString("definition");
                    databaseVisitor.moduleDefinition(objectIdentifier, modifyDate, definition);
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
