package org.dandoy.dbpop.download;

import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ForeignKey;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExecutionPlan implements AutoCloseable {
    private final Database database;
    private final File datasetsDirectory;
    private final String dataset;
    private final Set<TableName> processed = new HashSet<>();
    private final List<ExecutionNode> executionNodes = new ArrayList<>();
    private TableDownloader tableDownloader;

    public ExecutionPlan(Database database, File datasetsDirectory, String dataset) {
        this.database = database;
        this.datasetsDirectory = datasetsDirectory;
        this.dataset = dataset;
    }

    @Override
    public void close() {
        flush();
        executionNodes.forEach(ExecutionNode::close);
        tableDownloader.close();
    }

    private void flush() {
        boolean hasFlushed;
        do {
            hasFlushed = false;
            for (ExecutionNode executionNode : executionNodes) {
                if (executionNode.flush()) {
                    hasFlushed = true;
                }
            }
        } while (hasFlushed);
    }

    public void download(Set<List<Object>> pks) {
        tableDownloader.download(pks);
    }

    public void build(TableName tableName, List<String> filteredColumns) {
        processed.add(tableName);
        Table table = database.getTable(tableName);

        tableDownloader = TableDownloader.builder()
                .setDatabase(database)
                .setDatasetsDirectory(datasetsDirectory)
                .setDataset(dataset)
                .setTableName(table.tableName())
                .setFilteredColumns(filteredColumns)
                .build();
        List<SelectedColumn> selectedColumns = tableDownloader.getSelectedColumns();
        List<SelectedColumn> filterSelectedColumns = filteredColumns.stream().map(it -> SelectedColumn.findByName(selectedColumns, it)).toList();
        ExecutionNode executionNode = new ExecutionNode(table.tableName(), tableDownloader, filterSelectedColumns);
        addLookupNodes(executionNode, table);
        addDataNodes(executionNode, table);
    }

    private ExecutionNode buildData(TableName tableName, List<String> filteredColumns, List<SelectedColumn> pkSelectedColumns) {
        Table table = database.getTable(tableName);

        ExecutionNode executionNode = createExecutionNode(table, filteredColumns, pkSelectedColumns);
        addLookupNodes(executionNode, table);
        addDataNodes(executionNode, table);

        return executionNode;
    }

    private ExecutionNode buildLookup(TableName tableName, List<String> pkColumns, List<SelectedColumn> fkSelectedColumns) {
        Table table = database.getTable(tableName);

        ExecutionNode executionNode = createExecutionNode(table, pkColumns, fkSelectedColumns);
        addLookupNodes(executionNode, table);

        return executionNode;
    }

    private void addDataNodes(ExecutionNode parentExecutionNode, Table table) {
        // If we are on "invoices", fetch "invoice_details"
        for (ForeignKey foreignKey : database.getRelatedForeignKeys(table.tableName())) {
            TableName fkTableName = foreignKey.getFkTableName();
            if (processed.add(fkTableName)) {
                List<String> pkColumns = foreignKey.getPkColumns();
                List<String> fkColumns = foreignKey.getFkColumns();
                List<SelectedColumn> pkSelectedColumns = SelectedColumn.findByName(parentExecutionNode.getSelectedColumns(), pkColumns);
                ExecutionNode childExecutionNode = buildData(fkTableName, fkColumns, pkSelectedColumns);
                parentExecutionNode.addExecutionNode(childExecutionNode);
            }
        }
    }

    private void addLookupNodes(ExecutionNode parentExecutionNode, Table table) {
        // If we are on "invoices", fetch "customers"
        for (ForeignKey foreignKey : table.foreignKeys()) {
            TableName pkTableName = foreignKey.getPkTableName();
            if (processed.add(pkTableName)) {
                List<String> pkColumns = foreignKey.getPkColumns(); // customers.customer_id
                List<String> fkColumns = foreignKey.getFkColumns(); // invoices.customer_id
                List<SelectedColumn> fkSelectedColumns = SelectedColumn.findByName(parentExecutionNode.getSelectedColumns(), fkColumns);
                ExecutionNode childExecutionNode = buildLookup(pkTableName, pkColumns, fkSelectedColumns);
                parentExecutionNode.addExecutionNode(childExecutionNode);
            }
        }
    }

    /**
     * @param table                  The table to download from. For example: customers.
     * @param filteredColumns        The columns in the WHERE clause. For example, customers.customer_id.
     * @param extractSelectedColumns The columns to extract from the parent. For example, invoices.customer_id
     */
    @NotNull
    private ExecutionNode createExecutionNode(Table table, List<String> filteredColumns, List<SelectedColumn> extractSelectedColumns) {
        TableDownloader tableDownloader = TableDownloader.builder()
                .setDatabase(database)
                .setDatasetsDirectory(datasetsDirectory)
                .setDataset(dataset)
                .setTableName(table.tableName())
                .setFilteredColumns(filteredColumns)
                .build();
        ExecutionNode executionNode = new ExecutionNode(table.tableName(), tableDownloader, extractSelectedColumns);
        executionNodes.add(executionNode);
        return executionNode;
    }
}
