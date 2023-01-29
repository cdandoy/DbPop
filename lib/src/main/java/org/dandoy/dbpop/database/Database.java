package org.dandoy.dbpop.database;

import org.dandoy.dbpop.database.mssql.SqlServerDatabase;
import org.dandoy.dbpop.database.pgsql.PostgresDatabase;
import org.dandoy.dbpop.upload.DataFileHeader;
import org.dandoy.dbpop.utils.NotImplementedException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public abstract class Database implements AutoCloseable {

    public static final int ROW_COUNT_MAX = 1000;

    public static DatabaseProxy createDatabase(ConnectionBuilder connectionBuilder) {
        return createDatabase(connectionBuilder, VirtualFkCache.createVirtualFkCache());
    }

    public static DatabaseProxy createDatabase(ConnectionBuilder connectionBuilder, VirtualFkCache virtualFkCache) {
        DefaultDatabase defaultDatabase = createDefaultDatabase(connectionBuilder);
        return new DatabaseProxy(defaultDatabase, virtualFkCache);
    }

    public static DefaultDatabase createDefaultDatabase(ConnectionBuilder connectionBuilder) {
        try {
            Connection connection = connectionBuilder.createConnection();
            try {
                DatabaseMetaData metaData = connection.getMetaData();
                String databaseProductName = metaData.getDatabaseProductName();
                if ("Microsoft SQL Server".equals(databaseProductName)) {
                    return new SqlServerDatabase(connection);
                } else if ("PostgreSQL".equals(databaseProductName)) {
                    return new PostgresDatabase(connection);
                } else {
                    throw new RuntimeException("Unsupported database " + databaseProductName);
                }
            } catch (Exception e) {
                connection.close();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public abstract void close();

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

    public abstract String quote(Collection<String> strings);

    public abstract String quote(TableName tableName);

    public abstract String quote(String s);

    public abstract void deleteTable(TableName tableName);

    public abstract void deleteTable(Table table);

    public abstract DatabasePreparationFactory createDatabasePreparationFactory();

    public abstract boolean isBinary(ResultSetMetaData metaData, int i) throws SQLException;

    public abstract RowCount getRowCount(TableName tableName);

    public abstract void enableForeignKey(ForeignKey foreignKey);

    public abstract void disableForeignKey(ForeignKey foreignKey);

    public void createCatalog(String catalog) {
        throw new NotImplementedException();
    }

    public void createShema(String catalog, String schema) {
        throw new NotImplementedException();
    }
}
