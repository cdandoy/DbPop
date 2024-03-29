package org.dandoy.dbpop.database.mssql;

import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpop.utils.CollectionUtils;

import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.dandoy.dbpop.database.ObjectTypes.*;
import static org.dandoy.dbpop.database.mssql.SqlServerObjectTypes.TYPE;
import static org.dandoy.dbpop.database.mssql.SqlServerObjectTypes.TYPE_TABLE;

@SuppressWarnings("DuplicatedCode")
@Slf4j
public class SqlServerDatabaseIntrospector implements DatabaseIntrospector {
    private final Connection connection;
    private final SqlServerDatabase database;

    @SneakyThrows
    public SqlServerDatabaseIntrospector(SqlServerDatabase database) {
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
    public void visitModuleMetas(DatabaseVisitor databaseVisitor) {
        for (String catalog : database.getCatalogs()) {
            visitModuleMetas(catalog, databaseVisitor);
        }
    }

    @Override
    @SneakyThrows
    public void visitModuleMetas(String catalog, DatabaseVisitor databaseVisitor) {
        use(catalog);

        // Everything but Foreign Keys and indexes for which we need to fetch the parent table
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT o.object_id, s.name AS "schema", o.name, o.type_desc, o.modify_date AS modify_date
                FROM sys.schemas s
                         JOIN sys.objects o ON o.schema_id = s.schema_id
                WHERE s.name NOT IN ('temp')
                  AND o.type_desc IN ('USER_TABLE', 'SQL_INLINE_TABLE_VALUED_FUNCTION', 'SQL_SCALAR_FUNCTION', 'SQL_STORED_PROCEDURE', 'SQL_TABLE_VALUED_FUNCTION', 'SQL_TRIGGER', 'VIEW')
                  AND o.is_ms_shipped = 0
                UNION ALL
                -- SQL Server bug: TYPE_TABLE has is_ms_shipped=1
                SELECT o.object_id, s.name AS "schema", tt.name, o.type_desc, o.modify_date AS modify_date
                FROM sys.schemas s
                         JOIN sys.table_types tt ON tt.schema_id = s.schema_id
                         JOIN sys.objects o ON o.object_id = tt.type_table_object_id
                WHERE s.name NOT IN ('sys', 'temp')
                  AND o.type_desc IN ('TYPE_TABLE')
                -- AND o.is_ms_shipped = 0
                ORDER BY "schema", name
                """)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    int objectId = resultSet.getInt("object_id");
                    String schema = resultSet.getString("schema");
                    String name = resultSet.getString("name");
                    if (areValidNames(schema, name)) {
                        String typeDesc = resultSet.getString("type_desc");
                        Timestamp modifyDate = resultSet.getTimestamp("modify_date");
                        databaseVisitor.moduleMeta(new SqlServerObjectIdentifier(objectId, typeDesc, catalog, schema, name), modifyDate);
                    }
                }
            }
        }

        // Foreign Keys
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT o.object_id   AS fk_id,
                       s.name        AS fk_schema,
                       o.name        AS fk_name,
                       o.type_desc   AS fk_type,
                       o.modify_date AS modify_date,
                       t_o.object_id AS table_id,
                       s_o.name      AS table_schema,
                       t_o.name      AS table_name,
                       t_o.type_desc AS table_type
                FROM sys.schemas s
                         JOIN sys.objects o ON o.schema_id = s.schema_id
                         JOIN sys.foreign_keys fk ON fk.object_id = o.object_id
                         JOIN sys.objects t_o ON t_o.object_id = fk.parent_object_id
                         JOIN sys.schemas s_o ON s_o.schema_id = t_o.schema_id
                WHERE o.is_ms_shipped = 0
                  AND s.name NOT IN ('temp')
                  AND o.type_desc = 'FOREIGN_KEY_CONSTRAINT'
                ORDER BY s.name, o.name
                """)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    int fkId = resultSet.getInt("fk_id");
                    String fkSchema = resultSet.getString("fk_schema");
                    String fkName = resultSet.getString("fk_name");
                    String fkType = resultSet.getString("fk_type");
                    Timestamp modifyDate = resultSet.getTimestamp("modify_date");
                    int tableId = resultSet.getInt("table_id");
                    String tableSchema = resultSet.getString("table_schema");
                    String tableName = resultSet.getString("table_name");
                    String tableType = resultSet.getString("table_type");
                    if (areValidNames(fkSchema, fkName, tableSchema, tableName)) {
                        databaseVisitor.moduleMeta(
                                new SqlServerObjectIdentifier(
                                        fkId, fkType, catalog, fkSchema, fkName,
                                        new SqlServerObjectIdentifier(tableId, tableType, catalog, tableSchema, tableName)
                                ),
                                modifyDate
                        );
                    }
                }
            }
        }

        // Indexes
        // Tables without an index have a dummy unnamed in the sys.indexes table. Ignore that. [i.name IS NOT NULL]
        // Table valued functions can have a primary key, but it is declared in the TVF, so it can be ignored [o.type_desc NOT IN ('SQL_TABLE_VALUED_FUNCTION')]
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT o.object_id, s.name AS "schema", o.name AS "table", i.name AS "index", i.is_primary_key, o.modify_date AS modify_date
                FROM sys.schemas s
                         JOIN sys.objects o ON o.schema_id = s.schema_id
                         JOIN sys.indexes i ON i.object_id = o.object_id
                WHERE o.is_ms_shipped = 0
                  AND s.name NOT IN ('temp')
                  AND i.name IS NOT NULL
                  AND o.type_desc NOT IN ('SQL_TABLE_VALUED_FUNCTION')
                ORDER BY s.name, o.name
                """)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    int objectId = resultSet.getInt("object_id");
                    String schema = resultSet.getString("schema");
                    String tableName = resultSet.getString("table");
                    String indexName = resultSet.getString("index");
                    boolean isPrimaryKey = resultSet.getInt("is_primary_key") > 0;
                    Timestamp modifyDate = resultSet.getTimestamp("modify_date");
                    if (areValidNames(schema, tableName, indexName)) {
                        databaseVisitor.moduleMeta(
                                new SqlServerObjectIdentifier(
                                        null, isPrimaryKey ? PRIMARY_KEY : INDEX, catalog, schema, indexName,
                                        new SqlServerObjectIdentifier(objectId, USER_TABLE, catalog, schema, tableName)
                                ),
                                modifyDate
                        );
                    }
                }
            }
        }

        // Types
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT t.user_type_id,
                       s.name  AS schema_name,
                       t.name  AS type_name,
                       t.is_nullable,
                       t.max_length,
                       t.precision,
                       t.scale,
                       st.name AS system_type_name
                FROM sys.schemas s
                         JOIN sys.types t ON t.schema_id = s.schema_id
                         JOIN sys.types st ON st.system_type_id = t.system_type_id AND st.is_user_defined = 0
                WHERE t.is_user_defined = 1
                  AND t.is_table_type = 0
                  AND st.system_type_id = st.user_type_id
                ORDER BY t.name
                """)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    databaseVisitor.moduleMeta(
                            new SqlServerObjectIdentifier(
                                    resultSet.getInt("user_type_id"),
                                    TYPE,
                                    catalog,
                                    resultSet.getString("schema_name"),
                                    resultSet.getString("type_name")
                            ),
                            null
                    );
                }
            }
        }
    }

    private static boolean areValidNames(String... names) {
        for (String name : names) {
            if (StringUtils.containsAny(name, "[]")) return false;
        }
        return true;
    }

    @Override
    @SneakyThrows
    public void visitModuleDefinitions(DatabaseVisitor databaseVisitor) {
        Collection<String> catalogs = database.getCatalogs();
        for (String catalog : catalogs) {
            visitModuleDefinitions(catalog, databaseVisitor);
        }
    }

    @Override
    @SneakyThrows
    public void visitModuleDefinitions(String catalog, DatabaseVisitor databaseVisitor) {
        use(catalog);
        // Mostly sprocs
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT o.object_id, s.name AS "schema", o.name, o.type_desc, o.modify_date AS modify_date, sm.definition
                FROM sys.schemas s
                         JOIN sys.objects o ON o.schema_id = s.schema_id
                         LEFT JOIN sys.sql_modules sm ON sm.object_id = o.object_id
                WHERE o.is_ms_shipped = 0
                  AND o.type_desc IN ('SQL_INLINE_TABLE_VALUED_FUNCTION', 'SQL_SCALAR_FUNCTION', 'SQL_STORED_PROCEDURE', 'SQL_TABLE_VALUED_FUNCTION', 'SQL_TRIGGER', 'VIEW')
                  AND s.name NOT IN ('temp')
                ORDER BY s.name, o.name
                """)) {
            visitModuleDefinitions(databaseVisitor, catalog, preparedStatement);
        }

        // Tables
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT t.object_id        AS table_id,
                       s.name             AS schema_name,
                       t.name             AS table_name,
                       t.modify_date      AS modify_date,
                       c.name             AS column_name,
                       c.is_nullable      AS is_nullable,
                       ic.seed_value      AS seed_value,
                       ic.increment_value AS increment_value,
                       ty.is_user_defined AS is_user_defined,
                       ts.name            AS type_schema,
                       ty.name            AS type_name,
                       c.max_length       AS type_max_length,
                       c.precision        AS type_precision,
                       c.scale            AS type_scale,
                       dc.name            AS default_constraint_name,
                       dc.definition      AS default_value
                FROM sys.schemas s
                         JOIN sys.tables t ON t.schema_id = s.schema_id
                         JOIN sys.columns c ON c.object_id = t.object_id
                         LEFT JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                         LEFT JOIN sys.schemas ts ON ts.schema_id = ty.schema_id
                         LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.name = c.name
                         LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
                WHERE t.is_ms_shipped = 0
                  AND s.name NOT IN ('temp')
                ORDER BY s.name, t.name, c.column_id
                """)) {
            visitTableDefinitions(databaseVisitor, catalog, preparedStatement);
        }

        // Foreign Keys
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT t.object_id    AS table_id,
                       fk.object_id   AS fk_id,
                       s.name         AS s,
                       t.name         AS t,
                       fk.name        AS fk_name,
                       m_c.name       AS col,
                       r_s.name       AS ref_schema,
                       r_t.name       AS ref_table,
                       o_c.name       AS ref_col,
                       fk.modify_date AS modify_date
                FROM sys.schemas s
                         JOIN sys.tables t ON t.schema_id = s.schema_id
                         JOIN sys.foreign_keys fk ON fk.parent_object_id = t.object_id
                         JOIN sys.tables r_t ON r_t.object_id = fk.referenced_object_id
                         JOIN sys.schemas r_s ON r_s.schema_id = r_t.schema_id
                         JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                         JOIN sys.columns o_c ON o_c.object_id = fkc.referenced_object_id AND o_c.column_id = fkc.referenced_column_id
                         JOIN sys.columns m_c ON m_c.object_id = fkc.parent_object_id AND m_c.column_id = fkc.parent_column_id
                WHERE t.is_ms_shipped = 0
                  AND s.name NOT IN ('temp')
                ORDER BY fk.name, s.name, t.name, m_c.column_id
                                """)) {
            visitModuleForeignKeyDefinitions(databaseVisitor, catalog, preparedStatement);
        }

        // Indexes
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT t.object_id            AS table_id,
                       i.index_id             AS index_id,
                       s.name                 AS schema_name,
                       t.name                 AS table_name,
                       t.modify_date          AS modify_date,
                       i.name                 AS index_name,
                       i.is_unique,
                       i.is_primary_key,
                       i.type_desc,
                       xi.xml_index_type_description,
                       xi.secondary_type_desc AS using_xml_index_for_type,
                       ui.name                AS using_xml_index_name,
                       c.name                 AS column_name,
                       ic.is_included_column
                FROM sys.schemas s
                         JOIN sys.tables t ON t.schema_id = s.schema_id
                         JOIN sys.indexes i ON i.object_id = t.object_id
                         JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id
                         JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
                         LEFT JOIN sys.xml_indexes xi ON i.object_id = xi.object_id AND xi.index_id = i.index_id
                         LEFT JOIN sys.indexes ui ON ui.index_id = xi.using_xml_index_id AND ui.object_id = xi.object_id
                WHERE t.is_ms_shipped = 0
                  AND s.name NOT IN ('temp')
                ORDER BY s.name, t.name, i.index_id, ic.key_ordinal
                """)) {
            visitModuleIndexDefinitions(databaseVisitor, catalog, preparedStatement);
        }

        // Tables Types
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT tt.type_table_object_id AS table_id,
                       s.name                  AS schema_name,
                       tt.name                 AS table_name,
                       o.modify_date           AS modify_date,
                       c.name                  AS column_name,
                       c.is_nullable           AS is_nullable,
                       ic.seed_value           AS seed_value,
                       ic.increment_value      AS increment_value,
                       ty.is_user_defined      AS is_user_defined,
                       ts.name                 AS type_schema,
                       ty.name                 AS type_name,
                       c.max_length            AS type_max_length,
                       c.precision             AS type_precision,
                       c.scale                 AS type_scale,
                       dc.name                 AS default_constraint_name,
                       dc.definition           AS default_value
                FROM sys.schemas s
                         JOIN sys.table_types tt ON tt.schema_id = s.schema_id
                         JOIN sys.objects o ON o.object_id = tt.type_table_object_id
                         JOIN sys.columns c ON c.object_id = tt.type_table_object_id
                         LEFT JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                         LEFT JOIN sys.schemas ts ON ts.schema_id = ty.schema_id
                         LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.name = c.name
                         LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
                WHERE s.name NOT IN ('sys', 'temp')
                ORDER BY s.name, tt.name, c.column_id
                """)) {
            visitTableTypeDefinitions(databaseVisitor, catalog, preparedStatement);
        }

        // Types
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT t.user_type_id,
                       s.name  AS schema_name,
                       t.name  AS type_name,
                       t.is_nullable,
                       t.max_length,
                       t.precision,
                       t.scale,
                       st.name AS system_type_name
                FROM sys.schemas s
                         JOIN sys.types t ON t.schema_id = s.schema_id
                         JOIN sys.types st ON st.system_type_id = t.system_type_id AND st.is_user_defined = 0
                WHERE t.is_user_defined = 1
                  AND t.is_table_type = 0
                  AND st.system_type_id = st.user_type_id
                ORDER BY t.name
                """)) {
            visitTypeDefinitions(databaseVisitor, catalog, preparedStatement);
        }
    }

    @SneakyThrows
    private void visitModuleForeignKeyDefinitions(DatabaseVisitor databaseVisitor, String catalog, PreparedStatement preparedStatement) {
        @Setter
        @Accessors(chain = true)
        class Closer {
            Integer tableId = null;
            Integer fkId = null;
            String schema = null;
            String table = null;
            String fkName = null;
            String refSchema = null;
            String refTable = null;
            Date modifyDate = null;
            List<String> pkColumns = new ArrayList<>();
            List<String> fkColumns = new ArrayList<>();

            Closer setFkId(Integer fkId) {
                if (!fkId.equals(this.fkId)) {
                    flush();
                }
                this.fkId = fkId;
                return this;
            }

            void addColumns(String pkCol, String fkCol) {
                pkColumns.add(pkCol);
                fkColumns.add(fkCol);
            }

            void flush() {
                if (fkId != null) {
                    TableName pkTableName = new TableName(catalog, refSchema, refTable);
                    TableName fkTableName = new TableName(catalog, schema, table);
                    ForeignKey foreignKey = new ForeignKey(
                            fkName,
                            null,
                            pkTableName,
                            new ArrayList<>(pkColumns),
                            fkTableName,
                            new ArrayList<>(fkColumns));

                    String definition = Table.getForeignKeyDefinition(database, fkTableName, foreignKey);
                    SqlServerObjectIdentifier tableIdentifier = new SqlServerObjectIdentifier(tableId, USER_TABLE, catalog, schema, table, null);
                    ObjectIdentifier fkIdentifier = new SqlServerObjectIdentifier(fkId, FOREIGN_KEY_CONSTRAINT, catalog, schema, fkName, tableIdentifier);
                    databaseVisitor.moduleDefinition(fkIdentifier, modifyDate, definition);
                    pkColumns.clear();
                    fkColumns.clear();
                }
            }
        }

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            Closer closer = new Closer();
            while (resultSet.next()) {
                closer.setFkId(resultSet.getInt("fk_id"))
                        .setTableId(resultSet.getInt("table_id"))
                        .setSchema(resultSet.getString("s"))
                        .setTable(resultSet.getString("t"))
                        .setFkName(resultSet.getString("fk_name"))
                        .setModifyDate(resultSet.getTimestamp("modify_date"))
                        .setRefSchema(resultSet.getString("ref_schema"))
                        .setRefTable(resultSet.getString("ref_table"))
                        .addColumns(
                                resultSet.getString("ref_col"),
                                resultSet.getString("col")
                        );
            }
            closer.flush();
        }
    }

    @SneakyThrows
    private void visitModuleIndexDefinitions(DatabaseVisitor databaseVisitor, String catalog, PreparedStatement preparedStatement) {
        @Setter
        @Accessors(chain = true)
        class Closer {
            Integer tableId;
            Integer indexId;
            String schemaName;
            String tableName;
            Date modifyDate;
            String indexName;
            boolean isUnique;
            boolean isPrimaryKey;
            String typeDesc = null;
            String xmlTypeDesc;
            SqlServerIndex.UsingXmlIndexForType usingXmlIndexForType;
            String usingXmlIndexName;
            List<SqlServerIndex.SqlServerIndexColumn> columns = new ArrayList<>();

            public Closer setIds(int tableId, int indexId) {
                if (this.tableId != null && this.indexId != null && (tableId != this.tableId || indexId != this.indexId)) {
                    flush();
                }
                this.tableId = tableId;
                this.indexId = indexId;
                return this;
            }

            void addColumn(SqlServerIndex.SqlServerIndexColumn column) {
                columns.add(column);
            }

            void flush() {
                if (this.tableId != null && this.indexId != null) {
                    SqlServerIndex sqlServerIndex = new SqlServerIndex(
                            indexName,
                            new TableName(catalog, schemaName, tableName),
                            isUnique,
                            isPrimaryKey,
                            typeDesc,
                            xmlTypeDesc,
                            usingXmlIndexName,
                            usingXmlIndexForType,
                            new ArrayList<>(columns)
                    );

                    String definition = sqlServerIndex.toDDL(database);
                    ObjectIdentifier indexIdentifier = new SqlServerObjectIdentifier(
                            null, isPrimaryKey ? PRIMARY_KEY : INDEX, catalog, schemaName, indexName,
                            new SqlServerObjectIdentifier(tableId, USER_TABLE, catalog, schemaName, tableName, null)
                    );
                    databaseVisitor.moduleDefinition(indexIdentifier, modifyDate, definition);
                    columns.clear();
                }
            }

            public Closer setUsingXmlIndexForType(String usingXmlIndexForType) {
                this.usingXmlIndexForType = usingXmlIndexForType == null ? null : SqlServerIndex.UsingXmlIndexForType.valueOf(usingXmlIndexForType);
                return this;
            }
        }

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            Closer closer = new Closer();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("schema_name");
                String tableName = resultSet.getString("table_name");
                String indexName = resultSet.getString("index_name");
                if (areValidNames(schemaName, tableName, indexName)) {
                    closer.setIds(
                                    resultSet.getInt("table_id"),
                                    resultSet.getInt("index_id")
                            )
                            .setSchemaName(schemaName)
                            .setTableName(tableName)
                            .setModifyDate(resultSet.getTimestamp("modify_date"))
                            .setIndexName(indexName)
                            .setUnique(resultSet.getInt("is_unique") > 0)
                            .setPrimaryKey(resultSet.getInt("is_primary_key") > 0)
                            .setTypeDesc(resultSet.getString("type_desc"))
                            .setXmlTypeDesc(resultSet.getString("xml_index_type_description"))
                            .setUsingXmlIndexForType(resultSet.getString("using_xml_index_for_type"))
                            .setUsingXmlIndexName(resultSet.getString("using_xml_index_name"))
                            .addColumn(new SqlServerIndex.SqlServerIndexColumn(
                                    resultSet.getString("column_name"),
                                    resultSet.getInt("is_included_column") > 0
                            ));
                }
            }
            closer.flush();
        }
    }

    public void visitModuleDefinitions(Collection<ObjectIdentifier> objectIdentifiers, DatabaseVisitor databaseVisitor) {
        objectIdentifiers.stream()
                .collect(Collectors.groupingBy(ObjectIdentifier::getCatalog))
                .forEach((catalog, catalogObjectIdentifier) -> {
                    if (use(catalog)) {
                        visitModuleDefinitions(databaseVisitor, catalog, catalogObjectIdentifier);
                    }
                });
    }

    private void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog, Collection<ObjectIdentifier> objectIdentifiers) {
        Collection<SqlServerObjectIdentifier> sqlServerObjectIdentifiers = toSqlServerObjectIdentifier(objectIdentifiers);
        List<SqlServerObjectIdentifier> regularIdentifiers = new ArrayList<>();
        List<SqlServerObjectIdentifier> tableIdentifiers = new ArrayList<>();
        List<SqlServerObjectIdentifier> fkIdentifiers = new ArrayList<>();
        List<SqlServerObjectIdentifier> indexIdentifiers = new ArrayList<>();
        List<SqlServerObjectIdentifier> typeTableIdentifiers = new ArrayList<>();
        List<SqlServerObjectIdentifier> typeIdentifiers = new ArrayList<>();
        for (SqlServerObjectIdentifier sqlServerObjectIdentifier : sqlServerObjectIdentifiers) {
            switch (sqlServerObjectIdentifier.getType()) {
                case USER_TABLE -> tableIdentifiers.add(sqlServerObjectIdentifier);
                case FOREIGN_KEY_CONSTRAINT -> fkIdentifiers.add(sqlServerObjectIdentifier);
                case PRIMARY_KEY, INDEX -> indexIdentifiers.add(sqlServerObjectIdentifier);
                case TYPE_TABLE -> typeTableIdentifiers.add(sqlServerObjectIdentifier);
                case TYPE -> typeIdentifiers.add(sqlServerObjectIdentifier);
                default -> regularIdentifiers.add(sqlServerObjectIdentifier);
            }
        }

        int bindCount = 1000;
        int indexBindCount = 500;
        CollectionUtils.partition(regularIdentifiers, bindCount, identifiers -> visitModuleDefinitions(databaseVisitor, catalog, identifiers, bindCount));
        CollectionUtils.partition(tableIdentifiers, bindCount, identifiers -> visitTableDefinitions(databaseVisitor, catalog, identifiers, bindCount));
        CollectionUtils.partition(fkIdentifiers, bindCount, identifiers -> visitForeignKeyDefinitions(databaseVisitor, catalog, identifiers, bindCount));
        CollectionUtils.partition(indexIdentifiers, indexBindCount, identifiers -> visitIndexDefinitions(databaseVisitor, catalog, identifiers, indexBindCount));
        CollectionUtils.partition(typeTableIdentifiers, indexBindCount, identifiers -> visitTypeTableDefinitions(databaseVisitor, catalog, identifiers, indexBindCount));
        CollectionUtils.partition(typeIdentifiers, indexBindCount, identifiers -> visitTypeDefinitions(databaseVisitor, catalog, identifiers, indexBindCount));
    }

    private List<SqlServerObjectIdentifier> toSqlServerObjectIdentifier(Collection<ObjectIdentifier> objectIdentifiers) {
        List<SqlServerObjectIdentifier> ret = new ArrayList<>();
        List<ObjectIdentifier> todo = new ArrayList<>();
        List<ObjectIdentifier> todoWithParent = new ArrayList<>();
        List<ObjectIdentifier> todoIndexes = new ArrayList<>();

        for (ObjectIdentifier objectIdentifier : objectIdentifiers) {
            if (objectIdentifier instanceof SqlServerObjectIdentifier sqlServerObjectIdentifier) {
                ret.add(sqlServerObjectIdentifier);
            } else if (objectIdentifier.getParent() == null) {
                todo.add(objectIdentifier);
            } else if (objectIdentifier.getType().equals("INDEX")) {
                todoIndexes.add(objectIdentifier);
            } else {
                todoWithParent.add(objectIdentifier);
            }
        }

        if (!todo.isEmpty()) {
            List<SqlServerObjectIdentifier> list = toSqlServerObjectIdentifiers(todo);
            ret.addAll(list);
        }

        if (!todoWithParent.isEmpty()) {
            List<SqlServerObjectIdentifier> list = toSqlServerObjectIdentifiersWithParents(todoWithParent);
            ret.addAll(list);
        }

        if (!todoIndexes.isEmpty()) {
            List<SqlServerObjectIdentifier> list = toIndexSqlServerObjectIdentifiers(todoIndexes);
            ret.addAll(list);
        }
        return ret;
    }

    private List<SqlServerObjectIdentifier> toSqlServerObjectIdentifiers(Collection<ObjectIdentifier> objectIdentifiers) {
        List<SqlServerObjectIdentifier> ret = new ArrayList<>();
        List<ObjectIdentifier> types = new ArrayList<>();
        List<ObjectIdentifier> objects = new ArrayList<>();

        for (ObjectIdentifier objectIdentifier : objectIdentifiers) {
            if (objectIdentifier.getType().equals(TYPE)) {
                types.add(objectIdentifier);
            } else {
                objects.add(objectIdentifier);
            }
        }

        if (!types.isEmpty()) {
            ret.addAll(toSqlServerObjectIdentifiers_Types(types));
        }
        if (!objects.isEmpty()) {
            ret.addAll(toSqlServerObjectIdentifiers_Objects(objects));
        }

        return ret;
    }

    private List<SqlServerObjectIdentifier> toSqlServerObjectIdentifiers_Objects(Collection<ObjectIdentifier> objectIdentifiers) {
        List<SqlServerObjectIdentifier> ret = new ArrayList<>();

        try {
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT o.object_id
                    FROM sys.schemas s
                             JOIN sys.objects o ON s.schema_id = o.schema_id
                    WHERE s.name = ?
                      AND o.name = ?;
                    """)) {
                for (Map.Entry<String, List<ObjectIdentifier>> entry : objectIdentifiers.stream()
                        .collect(Collectors.groupingBy(ObjectIdentifier::getCatalog)).entrySet()) {
                    String catalog = entry.getKey();
                    database.useCatalog(catalog);
                    List<ObjectIdentifier> list = entry.getValue();
                    for (ObjectIdentifier objectIdentifier : list) {
                        preparedStatement.setString(1, objectIdentifier.getSchema());
                        preparedStatement.setString(2, objectIdentifier.getName());
                        try (ResultSet resultSet = preparedStatement.executeQuery()) {
                            if (resultSet.next()) {
                                int objectId = resultSet.getInt("object_id");
                                ret.add(
                                        new SqlServerObjectIdentifier(
                                                objectId,
                                                objectIdentifier.getType(),
                                                objectIdentifier.getCatalog(),
                                                objectIdentifier.getSchema(),
                                                objectIdentifier.getName()
                                        )
                                );
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    @SneakyThrows
    private List<SqlServerObjectIdentifier> toSqlServerObjectIdentifiers_Types(Collection<ObjectIdentifier> objectIdentifiers) {
        List<SqlServerObjectIdentifier> ret = new ArrayList<>();

        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT t.user_type_id,
                       s.name  AS schema_name,
                       t.name  AS type_name,
                       t.is_nullable,
                       t.max_length,
                       t.precision,
                       t.scale,
                       st.name AS system_type_name
                FROM sys.schemas s
                         JOIN sys.types t ON t.schema_id = s.schema_id
                         JOIN sys.types st ON st.system_type_id = t.system_type_id AND st.is_user_defined = 0
                WHERE t.is_user_defined = 1
                  AND t.is_table_type = 0
                  AND st.system_type_id = st.user_type_id
                ORDER BY t.name
                """)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    for (ObjectIdentifier objectIdentifier : objectIdentifiers) {
                        int userTypeId = resultSet.getInt("user_type_id");
                        String schemaName = resultSet.getString("schema_name");
                        String typeName = resultSet.getString("type_name");
                        if (objectIdentifier.getSchema().equals(schemaName) && objectIdentifier.getName().equals(typeName)) {
                            ret.add(new SqlServerObjectIdentifier(
                                    userTypeId, objectIdentifier.getType(), objectIdentifier.getCatalog(), objectIdentifier.getSchema(), objectIdentifier.getName()
                            ));
                        }
                    }
                }
            }
        }
        return ret;
    }

    private List<SqlServerObjectIdentifier> toSqlServerObjectIdentifiersWithParents(Collection<ObjectIdentifier> objectIdentifiers) {
        List<SqlServerObjectIdentifier> ret = new ArrayList<>();

        try {
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT o.object_id, o.parent_object_id
                    FROM sys.schemas s
                             JOIN sys.objects o ON s.schema_id = o.schema_id
                             JOIN sys.objects po ON po.object_id = o.parent_object_id
                             JOIN sys.schemas ps ON ps.schema_id = po.schema_id
                    WHERE s.name = ?
                      AND o.name = ?
                      AND ps.name = ?
                      AND po.name = ?
                    """)) {
                for (Map.Entry<String, List<ObjectIdentifier>> entry : objectIdentifiers.stream()
                        .collect(Collectors.groupingBy(ObjectIdentifier::getCatalog)).entrySet()) {
                    String catalog = entry.getKey();
                    database.useCatalog(catalog);
                    List<ObjectIdentifier> list = entry.getValue();
                    for (ObjectIdentifier objectIdentifier : list) {
                        preparedStatement.setString(1, objectIdentifier.getSchema());
                        preparedStatement.setString(2, objectIdentifier.getName());
                        ObjectIdentifier parentObjectIdentifier = objectIdentifier.getParent();
                        preparedStatement.setString(3, parentObjectIdentifier.getSchema());
                        preparedStatement.setString(4, parentObjectIdentifier.getName());
                        try (ResultSet resultSet = preparedStatement.executeQuery()) {
                            if (resultSet.next()) {
                                int objectId = resultSet.getInt("object_id");
                                int parentObjectId = resultSet.getInt("parent_object_id");
                                ret.add(
                                        new SqlServerObjectIdentifier(
                                                objectId,
                                                objectIdentifier.getType(),
                                                objectIdentifier.getCatalog(),
                                                objectIdentifier.getSchema(),
                                                objectIdentifier.getName(),
                                                new SqlServerObjectIdentifier(
                                                        parentObjectId,
                                                        parentObjectIdentifier.getType(),
                                                        parentObjectIdentifier.getCatalog(),
                                                        parentObjectIdentifier.getSchema(),
                                                        parentObjectIdentifier.getName()
                                                )
                                        )
                                );
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    private List<SqlServerObjectIdentifier> toIndexSqlServerObjectIdentifiers(Collection<ObjectIdentifier> objectIdentifiers) {
        List<SqlServerObjectIdentifier> ret = new ArrayList<>();

        try {
            try (PreparedStatement preparedStatement = connection.prepareStatement("""
                    SELECT t.object_id
                    FROM sys.schemas s
                             JOIN sys.tables t ON s.schema_id = t.schema_id
                             JOIN sys.indexes i ON i.object_id = t.object_id
                    WHERE s.name = ?
                      AND t.name = ?
                      AND i.name = ?
                    """)) {
                for (Map.Entry<String, List<ObjectIdentifier>> entry : objectIdentifiers.stream()
                        .collect(Collectors.groupingBy(ObjectIdentifier::getCatalog)).entrySet()) {
                    String catalog = entry.getKey();
                    database.useCatalog(catalog);
                    List<ObjectIdentifier> list = entry.getValue();
                    for (ObjectIdentifier objectIdentifier : list) {
                        ObjectIdentifier parentObjectIdentifier = objectIdentifier.getParent();
                        preparedStatement.setString(1, parentObjectIdentifier.getSchema());
                        preparedStatement.setString(2, parentObjectIdentifier.getName());
                        preparedStatement.setString(3, objectIdentifier.getName());
                        try (ResultSet resultSet = preparedStatement.executeQuery()) {
                            if (resultSet.next()) {
                                int parentObjectId = resultSet.getInt("object_id");
                                ret.add(
                                        new SqlServerObjectIdentifier(
                                                null,
                                                objectIdentifier.getType(),
                                                objectIdentifier.getCatalog(),
                                                objectIdentifier.getSchema(),
                                                objectIdentifier.getName(),
                                                new SqlServerObjectIdentifier(
                                                        parentObjectId,
                                                        parentObjectIdentifier.getType(),
                                                        parentObjectIdentifier.getCatalog(),
                                                        parentObjectIdentifier.getSchema(),
                                                        parentObjectIdentifier.getName()
                                                )
                                        )
                                );
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    /**
     * visitModuleDefinitions for everything but foreign keys and indexes
     */
    @SneakyThrows
    private void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog, List<SqlServerObjectIdentifier> identifiers, int bindCount) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT o.object_id, s.name AS "schema", o.name, o.type_desc, o.modify_date as modify_date, sm.definition
                FROM sys.schemas s
                         JOIN sys.objects o ON o.schema_id = s.schema_id
                         LEFT JOIN sys.sql_modules sm ON sm.object_id = o.object_id
                WHERE o.object_id in (%s)
                ORDER BY s.name, o.name
                """.formatted(StringUtils.repeat("?", ",", bindCount)))) {
            int i;
            for (i = 0; i < bindCount && i < identifiers.size(); i++) {
                preparedStatement.setInt(i + 1, identifiers.get(i).getObjectId());
            }
            for (; i < bindCount; i++) {
                preparedStatement.setNull(i + 1, Types.INTEGER);
            }
            visitModuleDefinitions(databaseVisitor, catalog, preparedStatement);
        }
    }

    /**
     * visitModuleDefinitions for tables
     */
    @SneakyThrows
    private void visitTableDefinitions(DatabaseVisitor databaseVisitor, String catalog, List<SqlServerObjectIdentifier> identifiers, int bindCount) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT t.object_id          AS table_id,
                       s.name               AS schema_name,
                       t.name               AS table_name,
                       t.modify_date        AS modify_date,
                       c.name               AS column_name,
                       c.is_nullable        AS is_nullable,
                       ic.seed_value        AS seed_value,
                       ic.increment_value   AS increment_value,
                       ty.is_user_defined   AS is_user_defined,
                       ts.name              AS type_schema,
                       ty.name              AS type_name,
                       c.max_length         AS type_max_length,
                       c.precision          AS type_precision,
                       c.scale              AS type_scale,
                       dc.name              AS default_constraint_name,
                       dc.definition        AS default_value
                FROM sys.schemas s
                         JOIN sys.tables t ON t.schema_id = s.schema_id
                         JOIN sys.columns c ON c.object_id = t.object_id
                         LEFT JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                         LEFT JOIN sys.schemas ts ON ts.schema_id = ty.schema_id
                         LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.name = c.name
                         LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
                WHERE t.is_ms_shipped = 0
                  AND t.object_id in (%s)
                ORDER BY s.name, t.name, c.column_id
                """.formatted(StringUtils.repeat("?", ",", bindCount)))) {
            int i;
            for (i = 0; i < bindCount && i < identifiers.size(); i++) {
                preparedStatement.setInt(i + 1, identifiers.get(i).getObjectId());
            }
            for (; i < bindCount; i++) {
                preparedStatement.setNull(i + 1, Types.INTEGER);
            }
            visitTableDefinitions(databaseVisitor, catalog, preparedStatement);
        }
    }

    @SneakyThrows
    private void visitTableDefinitions(DatabaseVisitor databaseVisitor, String catalog, PreparedStatement preparedStatement) {
        @Setter
        @Accessors(chain = true)
        class Closer {
            Integer tableId;
            String schemaName;
            String tableName;
            Timestamp modifyDate;
            List<Column> sqlServerColumns = new ArrayList<>();

            public Closer setTableId(int tableId) {
                if (this.tableId != null && this.tableId != tableId)
                    flush();
                this.tableId = tableId;
                return this;
            }

            void addColumn(SqlServerColumn sqlServerColumn) {
                sqlServerColumns.add(sqlServerColumn);
            }

            void flush() {
                if (tableId != null) {
                    databaseVisitor.moduleDefinition(
                            new SqlServerObjectIdentifier(tableId, USER_TABLE, catalog, schemaName, tableName),
                            modifyDate,
                            new SqlServerTable(
                                    new TableName(catalog, schemaName, tableName),
                                    sqlServerColumns,
                                    emptyList(),
                                    null,
                                    emptyList()
                            ).tableDDL(database)
                    );
                    sqlServerColumns.clear();
                }
            }
        }

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            Closer closer = new Closer();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("schema_name");
                String tableName = resultSet.getString("table_name");
                String columnName = resultSet.getString("column_name");
                if (areValidNames(schemaName, tableName, columnName)) {
                    boolean isUserDefined = resultSet.getInt("is_user_defined") > 0;
                    String typeSchema = resultSet.getString("type_schema");
                    closer.setTableId(resultSet.getInt("table_id"))
                            .setSchemaName(schemaName)
                            .setTableName(tableName)
                            .setModifyDate(resultSet.getTimestamp("modify_date"))
                            .addColumn(
                                    new SqlServerColumn(
                                            columnName,
                                            ColumnType.getColumnType(
                                                    resultSet.getString("type_name"),
                                                    resultSet.getInt("type_precision")
                                            ),
                                            resultSet.getInt("is_nullable") > 0,
                                            isUserDefined ? typeSchema : null,
                                            resultSet.getString("type_name"),
                                            resultSet.getInt("type_precision"),
                                            resultSet.getInt("type_max_length"),
                                            resultSet.getInt("type_scale"),
                                            resultSet.getString("seed_value"),
                                            resultSet.getString("increment_value"),
                                            resultSet.getString("default_constraint_name"),
                                            resultSet.getString("default_value")
                                    )
                            );
                }
            }
            closer.flush();
        }
    }

    /**
     * visitModuleDefinitions for type tables
     */
    @SneakyThrows
    private void visitTypeTableDefinitions(DatabaseVisitor databaseVisitor, String catalog, List<SqlServerObjectIdentifier> identifiers, int bindCount) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT tt.type_table_object_id  AS table_id,
                       s.name                   AS schema_name,
                       tt.name                  AS table_name,
                       o.modify_date            AS modify_date,
                       c.name                   AS column_name,
                       c.is_nullable            AS is_nullable,
                       ic.seed_value            AS seed_value,
                       ic.increment_value       AS increment_value,
                       ty.is_user_defined       AS is_user_defined,
                       ts.name                  AS type_schema,
                       ty.name                  AS type_name,
                       c.max_length             AS type_max_length,
                       c.precision              AS type_precision,
                       c.scale                  AS type_scale,
                       dc.name                  AS default_constraint_name,
                       dc.definition            AS default_value
                FROM sys.schemas s
                         JOIN sys.table_types tt ON tt.schema_id = s.schema_id
                         JOIN sys.objects o ON o.object_id = tt.type_table_object_id
                         JOIN sys.columns c ON c.object_id = tt.type_table_object_id
                         LEFT JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                         LEFT JOIN sys.schemas ts ON ts.schema_id = ty.schema_id
                         LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.name = c.name
                         LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
                WHERE o.object_id IN (%s)
                ORDER BY s.name, tt.name, c.column_id
                """.formatted(StringUtils.repeat("?", ",", bindCount)))) {
            int i;
            for (i = 0; i < bindCount && i < identifiers.size(); i++) {
                preparedStatement.setInt(i + 1, identifiers.get(i).getObjectId());
            }
            for (; i < bindCount; i++) {
                preparedStatement.setNull(i + 1, Types.INTEGER);
            }
            visitTableTypeDefinitions(databaseVisitor, catalog, preparedStatement);
        }
    }

    @SneakyThrows
    private void visitTableTypeDefinitions(DatabaseVisitor databaseVisitor, String catalog, PreparedStatement preparedStatement) {
        @Setter
        @Accessors(chain = true)
        class Closer {
            Integer tableId;
            String schemaName;
            String tableName;
            Timestamp modifyDate;
            List<Column> sqlServerColumns = new ArrayList<>();

            public Closer setTableId(int tableId) {
                if (this.tableId != null && this.tableId != tableId)
                    flush();
                this.tableId = tableId;
                return this;
            }

            void addColumn(SqlServerColumn sqlServerColumn) {
                sqlServerColumns.add(sqlServerColumn);
            }

            void flush() {
                if (tableId != null) {
                    databaseVisitor.moduleDefinition(
                            new SqlServerObjectIdentifier(tableId, TYPE_TABLE, catalog, schemaName, tableName),
                            modifyDate,
                            new SqlServerTableType(
                                    new TableName(catalog, schemaName, tableName),
                                    sqlServerColumns,
                                    emptyList(),
                                    null,
                                    emptyList()
                            ).tableDDL(database)
                    );
                    sqlServerColumns.clear();
                }
            }
        }

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            Closer closer = new Closer();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("schema_name");
                String tableName = resultSet.getString("table_name");
                String columnName = resultSet.getString("column_name");
                if (areValidNames(schemaName, tableName, columnName)) {
                    boolean isUserDefined = resultSet.getInt("is_user_defined") > 0;
                    String typeSchema = resultSet.getString("type_schema");
                    closer.setTableId(resultSet.getInt("table_id"))
                            .setSchemaName(schemaName)
                            .setTableName(tableName)
                            .setModifyDate(resultSet.getTimestamp("modify_date"))
                            .addColumn(
                                    new SqlServerColumn(
                                            columnName,
                                            ColumnType.getColumnType(
                                                    resultSet.getString("type_name"),
                                                    resultSet.getInt("type_precision")
                                            ),
                                            resultSet.getInt("is_nullable") > 0,
                                            isUserDefined ? typeSchema : null,
                                            resultSet.getString("type_name"),
                                            resultSet.getInt("type_precision"),
                                            resultSet.getInt("type_max_length"),
                                            resultSet.getInt("type_scale"),
                                            resultSet.getString("seed_value"),
                                            resultSet.getString("increment_value"),
                                            resultSet.getString("default_constraint_name"),
                                            resultSet.getString("default_value")
                                    )
                            );
                }
            }
            closer.flush();
        }
    }

    /**
     * visitModuleDefinitions for types
     */
    @SneakyThrows
    private void visitTypeDefinitions(DatabaseVisitor databaseVisitor, String catalog, List<SqlServerObjectIdentifier> identifiers, int bindCount) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                 SELECT t.user_type_id,
                        s.name  AS schema_name,
                        t.name  AS type_name,
                        t.is_nullable,
                        t.max_length,
                        t.precision,
                        t.scale,
                        st.name AS system_type_name
                 FROM sys.schemas s
                          JOIN sys.types t ON t.schema_id = s.schema_id
                          JOIN sys.types st ON st.system_type_id = t.system_type_id AND st.is_user_defined = 0
                 WHERE t.is_user_defined = 1
                   AND t.is_table_type = 0
                   AND st.system_type_id = st.user_type_id
                   AND t.user_type_id IN (%s)
                """.formatted(StringUtils.repeat("?", ",", bindCount)))) {
            int i;
            for (i = 0; i < bindCount && i < identifiers.size(); i++) {
                preparedStatement.setInt(i + 1, identifiers.get(i).getObjectId());
            }
            for (; i < bindCount; i++) {
                preparedStatement.setNull(i + 1, Types.INTEGER);
            }
            visitTypeDefinitions(databaseVisitor, catalog, preparedStatement);
        }
    }

    @SneakyThrows
    private void visitTypeDefinitions(DatabaseVisitor databaseVisitor, String catalog, PreparedStatement preparedStatement) {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                int userTypeId = resultSet.getInt("user_type_id");
                String schema = resultSet.getString("schema_name");
                String typeName = resultSet.getString("type_name");
                boolean isNullable = resultSet.getInt("is_nullable") > 0;
                int maxLength = resultSet.getInt("max_length");
                int precision = resultSet.getInt("precision");
                int scale = resultSet.getInt("scale");
                String systemTypeName = resultSet.getString("system_type_name");
                String definition = new SqlServerType(schema, typeName, systemTypeName, precision, maxLength, scale, isNullable)
                        .toDDL(database);
                databaseVisitor.moduleDefinition(
                        new SqlServerObjectIdentifier(userTypeId, TYPE, catalog, schema, typeName),
                        null,
                        definition
                );
            }
        }
    }

    /**
     * visitModuleDefinitions for foreign keys
     */
    @SneakyThrows
    private void visitForeignKeyDefinitions(DatabaseVisitor databaseVisitor, String catalog, List<SqlServerObjectIdentifier> identifiers, int bindCount) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT t.object_id  AS table_id,
                       fk.object_id AS fk_id,
                       fk.modify_date,
                       s.name       AS s,
                       t.name       AS t,
                       fk.name      AS fk_name,
                       m_c.name     AS col,
                       r_s.name     AS ref_schema,
                       r_t.name     AS ref_table,
                       o_c.name     AS ref_col
                FROM sys.schemas s
                         JOIN sys.tables t ON t.schema_id = s.schema_id
                         JOIN sys.foreign_keys fk ON fk.parent_object_id = t.object_id
                         JOIN sys.tables r_t ON r_t.object_id = fk.referenced_object_id
                         JOIN sys.schemas r_s ON r_s.schema_id = r_t.schema_id
                         JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                         JOIN sys.columns o_c ON o_c.object_id = fkc.referenced_object_id AND o_c.column_id = fkc.referenced_column_id
                         JOIN sys.columns m_c ON m_c.object_id = fkc.parent_object_id AND m_c.column_id = fkc.parent_column_id
                WHERE t.is_ms_shipped = 0
                  AND fk.object_id in (%s)
                ORDER BY fk.name, s.name, t.name, m_c.column_id
                """.formatted(StringUtils.repeat("?", ",", bindCount)))) {
            int i;
            for (i = 0; i < bindCount && i < identifiers.size(); i++) {
                preparedStatement.setInt(i + 1, identifiers.get(i).getObjectId());
            }
            for (; i < bindCount; i++) {
                preparedStatement.setNull(i + 1, Types.INTEGER);
            }
            visitModuleForeignKeyDefinitions(databaseVisitor, catalog, preparedStatement);
        }
    }

    /**
     * visitModuleDefinitions for indexes
     */
    @SneakyThrows
    private void visitIndexDefinitions(DatabaseVisitor databaseVisitor, String catalog, List<SqlServerObjectIdentifier> identifiers, int bindCount) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT t.object_id                      AS table_id,
                       i.index_id                       AS index_id,
                       s.name                           AS schema_name,
                       t.name                           AS table_name,
                       t.modify_date                    AS modify_date,
                       i.name                           AS index_name,
                       i.is_unique,
                       i.is_primary_key,
                       i.type_desc,
                       xi.xml_index_type_description,
                       xi.secondary_type_desc           AS using_xml_index_for_type,
                       ui.name                          AS using_xml_index_name,
                       c.name                           AS column_name,
                       ic.is_included_column
                FROM sys.schemas s
                         JOIN sys.tables t ON t.schema_id = s.schema_id
                         JOIN sys.indexes i ON i.object_id = t.object_id
                         JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id
                         JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
                         LEFT JOIN sys.xml_indexes xi ON i.object_id = xi.object_id AND xi.index_id = i.index_id
                         LEFT JOIN sys.indexes ui ON ui.index_id = xi.using_xml_index_id AND ui.object_id = xi.object_id
                         JOIN (VALUES %s) AS x(t, i) ON x.t = i.object_id AND x.i = i.name
                ORDER BY s.name, t.name, i.index_id, ic.key_ordinal
                """.formatted(StringUtils.repeat("(?, ?)", ",", bindCount)))) {
            int jdbcPos = 1;
            for (SqlServerObjectIdentifier indexIdentifier : identifiers) {
                SqlServerObjectIdentifier tableIdentifier = indexIdentifier.getParent();
                preparedStatement.setInt(jdbcPos++, tableIdentifier.getObjectId());
                preparedStatement.setString(jdbcPos++, indexIdentifier.getName());
            }
            while (jdbcPos < bindCount * 2) {
                preparedStatement.setNull(jdbcPos++, Types.INTEGER);
                preparedStatement.setNull(jdbcPos++, Types.VARCHAR);
            }
            visitModuleIndexDefinitions(databaseVisitor, catalog, preparedStatement);
        }
    }

    private void visitModuleDefinitions(DatabaseVisitor databaseVisitor, String catalog, PreparedStatement preparedStatement) throws SQLException {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                int objectId = resultSet.getInt("object_id");
                String schema = resultSet.getString("schema");
                String name = resultSet.getString("name");
                if (areValidNames(schema, name)) {
                    String typeDesc = resultSet.getString("type_desc");
                    Date modifyDate = resultSet.getTimestamp("modify_date");
                    SqlServerObjectIdentifier objectIdentifier = new SqlServerObjectIdentifier(objectId, typeDesc, catalog, schema, name);
                    String definition = resultSet.getString("definition");
                    databaseVisitor.moduleDefinition(objectIdentifier, modifyDate, definition);
                }
            }
        }
    }

    private boolean use(String catalog) {
        try (Statement statement = connection.createStatement()) {
            statement.execute("USE " + catalog);
            return true;
        } catch (SQLException e) {
            if (e.getSQLState().equals("S0001")) return false; // catalog doesn't exist
            throw new RuntimeException(e);
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
                  AND s.name NOT IN ('temp')
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
                    if (areValidNames(schema, name, dependentSchema, dependentName)) {
                        databaseVisitor.dependency(catalog, schema, name, typeDesc, dependentSchema, dependentName, dependentTypeDesc);
                    }
                }
            }
        }
    }
}
