package org.dandoy.dbpop.database;

import org.dandoy.dbpop.upload.DataFileHeader;
import org.dandoy.dbpop.upload.Dataset;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatabaseProxy extends Database {
    private final Database delegate;
    private final VirtualFkCache virtualFkCache;

    public DatabaseProxy(Database delegate, VirtualFkCache virtualFkCache) {
        this.delegate = delegate;
        this.virtualFkCache = virtualFkCache;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Connection getConnection() {
        return delegate.getConnection();
    }

    @Override
    public Collection<TableName> getTableNames(String catalog, String schema) {
        return delegate.getTableNames(catalog, schema);
    }

    @Override
    public Collection<Table> getTables() {
        return delegate.getTables();
    }

    @Override
    public Collection<Table> getTables(Set<TableName> datasetTableNames) {
        Collection<Table> tables = delegate.getTables(datasetTableNames);
        for (Table table : tables) {
            addVFKs(table);
        }
        return tables;
    }

    private void addVFKs(Table table) {
        List<ForeignKey> foreignKeys = virtualFkCache.findByFkTable(table.tableName());
        table.foreignKeys().addAll(foreignKeys);
    }

    @Override
    public Table getTable(TableName tableName) {
        Table table = delegate.getTable(tableName);
        addVFKs(table);
        return table;
    }

    @Override
    public List<ForeignKey> getRelatedForeignKeys(TableName tableName) {
        List<ForeignKey> relatedForeignKeys = delegate.getRelatedForeignKeys(tableName);
        List<ForeignKey> foreignKeys = virtualFkCache.findByPkTable(tableName);
        relatedForeignKeys.addAll(foreignKeys);
        return relatedForeignKeys;
    }

    @Override
    public List<String> getSchemas(String catalog) {
        return delegate.getSchemas(catalog);
    }

    @Override
    public void dropForeignKey(ForeignKey foreignKey) {
        ForeignKey virtualFk = virtualFkCache.getByPkTable(foreignKey.getPkTableName(), foreignKey.getName());
        if (virtualFk != null) {
            // How did we get here?
            throw new RuntimeException("Internal Error");
        }
        delegate.dropForeignKey(foreignKey);
    }

    @Override
    public void createForeignKey(ForeignKey foreignKey) {
        delegate.createForeignKey(foreignKey);
    }

    @Override
    public DefaultDatabase.DatabaseInserter createInserter(Table table, List<DataFileHeader> dataFileHeaders) throws SQLException {
        return delegate.createInserter(table, dataFileHeaders);
    }

    @Override
    public String quote(Collection<String> strings) {
        return delegate.quote(strings);
    }

    @Override
    public String quote(TableName tableName) {
        return delegate.quote(tableName);
    }

    @Override
    public String quote(String s) {
        return delegate.quote(s);
    }

    @Override
    public void deleteTable(Table table) {
        delegate.deleteTable(table);
    }

    @Override
    public DatabasePreparationStrategy createDatabasePreparationStrategy(Map<String, Dataset> datasetsByName, Map<TableName, Table> tablesByName, List<String> datasets) {
        return delegate.createDatabasePreparationStrategy(datasetsByName, tablesByName, datasets);
    }

    @Override
    public boolean isBinary(ResultSetMetaData metaData, int i) throws SQLException {
        return delegate.isBinary(metaData, i);
    }

    @Override
    protected Set<TableName> searchTableLike(String like) throws SQLException {
        return delegate.searchTableLike(like);
    }

    @Override
    public RowCount getRowCount(TableName tableName) {
        return delegate.getRowCount(tableName);
    }
}
