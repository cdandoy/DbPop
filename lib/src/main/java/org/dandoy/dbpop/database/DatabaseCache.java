package org.dandoy.dbpop.database;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.upload.DataFileHeader;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class DatabaseCache extends Database {
    private final Database delegate;
    private final VirtualFkCache virtualFkCache;
    private Map<TableName, Table> cache = null;

    public DatabaseCache(Database delegate, VirtualFkCache virtualFkCache) {
        this.delegate = delegate;
        this.virtualFkCache = virtualFkCache;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void verifyConnection() {
        delegate.verifyConnection();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return delegate.getConnection();
    }

    private static <T> List<T> concat(Collection<T> t1, Collection<T> t2) {
        ArrayList<T> ret = new ArrayList<>(t1);
        ret.addAll(t2);
        return ret;
    }

    private Table addVFKs(Table table) {
        if (table == null) return null;
        List<ForeignKey> virtualForeignKeys = virtualFkCache.findByFkTable(table.tableName());
        if (virtualForeignKeys.isEmpty()) return table;

        return new Table(
                table.tableName(),
                table.columns(),
                table.indexes(),
                table.primaryKey(),
                concat(
                        table.foreignKeys(),
                        virtualForeignKeys
                )
        );
    }

    @Override
    public Collection<TableName> getTableNames(String catalog, String schema) {
        return getTables().stream()
                .map(Table::tableName)
                .filter(it -> Objects.equals(schema, it.getSchema()) && Objects.equals(catalog, it.getCatalog()))
                .toList();
    }

    private Map<TableName, Table> getCache() {
        if (cache == null) {
            cache = new HashMap<>();
            Collection<Table> tables = delegate.getTables();
            for (Table table : tables) {
                cache.put(table.tableName(), table);
            }
        }
        return cache;
    }

    @Override
    public synchronized Collection<Table> getTables() {
        return getCache()
                .values().stream()
                .map(this::addVFKs)
                .toList();
    }

    @Override
    public Collection<Table> getTables(Set<TableName> datasetTableNames) {
        Map<TableName, Table> cache = getCache();
        return datasetTableNames.stream()
                .map(cache::get)
                .filter(Objects::nonNull)
                .toList();
    }

    @Override
    public Table getTable(TableName tableName) {
        return addVFKs(getCache().get(tableName));
    }

    @Override
    public List<ForeignKey> getRelatedForeignKeys(TableName tableName) {
        return getTables().stream()
                .flatMap(it -> it.foreignKeys().stream())
                .filter(it -> it.getPkTableName().equals(tableName))
                .toList();
    }

    @Override
    public List<String> getSchemas(String catalog) {
        return getCache()
                .keySet().stream()
                .filter(it -> Objects.equals(it.getCatalog(), catalog))
                .map(TableName::getSchema)
                .distinct()
                .toList();
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
            log.debug("{}: {}ms", tableName.toQualifiedName(), t1 - t0);
        }
    }

    @Override
    public void enableForeignKey(ForeignKey foreignKey) {
        if (!virtualFkCache.getForeignKeys().contains(foreignKey)) {
            delegate.enableForeignKey(foreignKey);
        }
    }

    @Override
    public void disableForeignKey(ForeignKey foreignKey) {
        if (!virtualFkCache.getForeignKeys().contains(foreignKey)) {
            delegate.disableForeignKey(foreignKey);
        }
    }
}
