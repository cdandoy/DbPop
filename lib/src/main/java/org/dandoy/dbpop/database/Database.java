package org.dandoy.dbpop.database;

import org.dandoy.dbpop.database.mssql.SqlServerDatabase;
import org.dandoy.dbpop.database.pgsql.PostgresDatabase;
import org.dandoy.dbpop.upload.DataFileHeader;
import org.dandoy.dbpop.upload.Dataset;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Database extends AutoCloseable {

    static DatabaseProxy createDatabase(ConnectionBuilder connectionBuilder) {
        return createDatabase(connectionBuilder, VirtualFkCache.createVirtualFkCache());
    }

    static DatabaseProxy createDatabase(ConnectionBuilder connectionBuilder, VirtualFkCache virtualFkCache) {
        DefaultDatabase defaultDatabase = createDefaultDatabase(connectionBuilder);
        return new DatabaseProxy(defaultDatabase, virtualFkCache);
    }

    private static DefaultDatabase createDefaultDatabase(ConnectionBuilder connectionBuilder) {
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
    void close();

    Connection getConnection();

    Collection<TableName> getTableNames(String catalog, String schema);

    Collection<Table> getTables(Set<TableName> datasetTableNames);

    Table getTable(TableName tableName);

    List<ForeignKey> getRelatedForeignKeys(TableName tableName);

    List<String> getSchemas(String catalog);

    void dropForeignKey(ForeignKey foreignKey);

    void createForeignKey(ForeignKey foreignKey);

    DefaultDatabase.DatabaseInserter createInserter(Table table, List<DataFileHeader> dataFileHeaders) throws SQLException;

    String quote(Collection<String> strings);

    String quote(TableName tableName);

    String quote(String s);

    void deleteTable(Table table);

    DatabasePreparationStrategy createDatabasePreparationStrategy(Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, List<String> datasets);

    boolean isBinary(ResultSetMetaData metaData, int i) throws SQLException;

    /**
     * Searches for tables by partial name
     */
    Set<TableName> searchTable(String query);
}
