package org.dandoy.dbpop.database;

import lombok.Getter;
import org.dandoy.dbpop.database.mssql.SqlServerDatabase;
import org.dandoy.dbpop.database.pgsql.PostgresDatabase;
import org.dandoy.dbpop.upload.DataFileHeader;
import org.dandoy.dbpop.utils.NotImplementedException;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public abstract class Database implements AutoCloseable {

    public static final int ROW_COUNT_MAX = 1000;
    public final TransitionGenerator INVALID_TRANSITION_GENERATOR;
    @Getter
    private final DatabaseVersion databaseVersion;

    protected Database(DatabaseVersion databaseVersion) {
        this.databaseVersion = databaseVersion;
        INVALID_TRANSITION_GENERATOR = new TransitionGenerator(this) {
            @Override
            public boolean isValid() {
                return false;
            }

            @Override
            protected void generateTransition(ObjectIdentifier objectIdentifier, String fromSql, String toSql, Transition transition) {
                transition.setError("%s Cannot generate the transition of %s", Database.this.getClass().getSimpleName(), objectIdentifier.toQualifiedName());
            }
        };
    }

    public static DatabaseProxy createDatabase(ConnectionBuilder connectionBuilder) {
        return createDatabase(connectionBuilder, VirtualFkCache.createVirtualFkCache());
    }

    public static DatabaseProxy createDatabase(ConnectionBuilder connectionBuilder, VirtualFkCache virtualFkCache) {
        DefaultDatabase defaultDatabase = createDefaultDatabase(connectionBuilder);
        return new DatabaseProxy(defaultDatabase, virtualFkCache);
    }

    public static DefaultDatabase createDefaultDatabase(ConnectionBuilder connectionBuilder) {
        try {
            String databaseProductName = getDatabaseProductName(connectionBuilder);
            if ("Microsoft SQL Server".equals(databaseProductName)) {
                return new SqlServerDatabase(connectionBuilder);
            } else if ("PostgreSQL".equals(databaseProductName)) {
                return new PostgresDatabase(connectionBuilder);
            } else {
                throw new RuntimeException("Unsupported database " + databaseProductName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getDatabaseProductName(ConnectionBuilder connectionBuilder) throws SQLException {
        try (Connection connection = connectionBuilder.createConnection()) {
            return connection.getMetaData().getDatabaseProductName();
        }
    }

    @Override
    public abstract void close();

    public boolean isSqlServer() {return false;}

    public abstract void verifyConnection();

    public abstract Connection getConnection();

    public abstract DatabaseIntrospector createDatabaseIntrospector();

    public abstract Collection<String> getCatalogs();

    public abstract Collection<TableName> getTableNames(String catalog, String schema);

    public abstract Collection<Table> getTables();

    public abstract Collection<Table> getTables(String catalog);

    public abstract Collection<Table> getTables(Set<TableName> datasetTableNames);

    public abstract Table getTable(TableName tableName);

    public abstract List<ForeignKey> getRelatedForeignKeys(TableName tableName);

    public abstract List<String> getSchemas(String catalog);

    public abstract void dropForeignKey(ForeignKey foreignKey);

    public abstract void createForeignKey(ForeignKey foreignKey);

    public abstract DefaultDatabase.DatabaseInserter createInserter(Table table, List<DataFileHeader> dataFileHeaders) throws SQLException;

    public abstract String quote(String delimiter, String... strings);

    public abstract String quote(String delimiter, Collection<String> strings);

    public abstract String quote(TableName tableName);

    public abstract String quote(ObjectIdentifier objectIdentifier);

    public abstract String quote(String s);

    public abstract void deleteTable(TableName tableName);

    public abstract void deleteTable(Table table);

    public abstract DatabasePreparationFactory createDatabasePreparationFactory();

    public abstract boolean isBinary(ResultSetMetaData metaData, int i) throws SQLException;

    public abstract RowCount getRowCount(TableName tableName);

    public abstract void enableForeignKey(ForeignKey foreignKey);

    public abstract void disableForeignKey(ForeignKey foreignKey);

    public TransitionGenerator getTransitionGenerator(String objectType) {
        return INVALID_TRANSITION_GENERATOR;
    }

    public void createCatalog(String catalog) {
        throw new NotImplementedException();
    }

    public void useCatalog(String catalog) {
        throw new NotImplementedException();
    }

    public void createShema(String catalog, String schema) {
        throw new NotImplementedException();
    }

    public abstract void dropObject(ObjectIdentifier objectIdentifier);

    public String getDefinition(ObjectIdentifier objectIdentifier) {
        List<String> definitions = new ArrayList<>();
        DatabaseIntrospector databaseIntrospector = createDatabaseIntrospector();
        databaseIntrospector.visitModuleDefinitions(List.of(objectIdentifier), new DatabaseVisitor() {
            @Override
            public void moduleDefinition(ObjectIdentifier objectIdentifier, Date modifyDate, @Nullable String definition) {
                definitions.add(definition);
            }
        });
        if (definitions.isEmpty()) {
            return null;
        } else {
            return definitions.get(0);
        }
    }

    public abstract long getEpochTime();
}
