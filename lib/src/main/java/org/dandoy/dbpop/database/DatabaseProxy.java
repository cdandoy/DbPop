package org.dandoy.dbpop.database;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.upload.DataFileHeader;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Slf4j
public class DatabaseProxy extends Database {
    private final Database delegate;
    private final VirtualFkCache virtualFkCache;

    public DatabaseProxy(Database delegate, VirtualFkCache virtualFkCache) {
        super(delegate.getDatabaseVersion());
        this.delegate = delegate;
        this.virtualFkCache = virtualFkCache;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean isSqlServer() {
        return delegate.isSqlServer();
    }

    @Override
    public void verifyConnection() {
        delegate.verifyConnection();
    }

    @Override
    public Connection getConnection() {
        return delegate.getConnection();
    }

    @Override
    public DatabaseIntrospector createDatabaseIntrospector() {
        return delegate.createDatabaseIntrospector();
    }

    @Override
    public Collection<String> getCatalogs() {
        return delegate.getCatalogs();
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
    public Collection<Table> getTables(String catalog) {
        return delegate.getTables(catalog);
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
        List<ForeignKey> foreignKeys = virtualFkCache.findByFkTable(table.getTableName());
        table.getForeignKeys().addAll(foreignKeys);
    }

    @Override
    public Table getTable(TableName tableName) {
        Table table = delegate.getTable(tableName);
        if (table != null) {
            addVFKs(table);
        }
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
    public String quote(String delimiter, String... strings) {
        return delegate.quote(delimiter, strings);
    }

    @Override
    public String quote(String delimiter, Collection<String> strings) {
        return delegate.quote(delimiter, strings);
    }

    @Override
    public String quote(TableName tableName) {
        return delegate.quote(tableName);
    }

    @Override
    public String quote(ObjectIdentifier objectIdentifier) {
        return delegate.quote(objectIdentifier);
    }

    @Override
    public String quote(String s) {
        return delegate.quote(s);
    }

    @Override
    public void deleteTable(TableName tableName) {
        delegate.deleteTable(tableName);
    }

    @Override
    public void deleteTable(Table table) {
        delegate.deleteTable(table);
    }

    @Override
    public DatabasePreparationFactory createDatabasePreparationFactory() {
        return delegate.createDatabasePreparationFactory();
    }

    @Override
    public boolean isBinary(ResultSetMetaData metaData, int i) throws SQLException {
        return delegate.isBinary(metaData, i);
    }

    @Override
    public RowCount getRowCount(TableName tableName) {
        long t0 = System.currentTimeMillis();
        try {
            return delegate.getRowCount(tableName);
        } finally {
            long t1 = System.currentTimeMillis();
            log.debug("RowCount {}: {}ms", tableName.toQualifiedName(), t1 - t0);
        }
    }

    @Override
    public void enableForeignKey(ForeignKey foreignKey) {
        delegate.enableForeignKey(foreignKey);
    }

    @Override
    public void disableForeignKey(ForeignKey foreignKey) {
        delegate.disableForeignKey(foreignKey);
    }

    @Override
    public TransitionGenerator getTransitionGenerator(String objectType) {
        return delegate.getTransitionGenerator(objectType);
    }

    @Override
    public void createCatalog(String catalog) {
        delegate.createCatalog(catalog);
    }

    @Override
    public void useCatalog(String catalog) {
        delegate.useCatalog(catalog);
    }

    @Override
    public void createShema(String catalog, String schema) {
        delegate.createShema(catalog, schema);
    }

    @Override
    public void dropObject(ObjectIdentifier objectIdentifier) {
        delegate.dropObject(objectIdentifier);
    }

    @Override
    public long getEpochTime() {
        return delegate.getEpochTime();
    }
}
