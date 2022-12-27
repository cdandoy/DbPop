package org.dandoy.dbpop.download;

import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpop.database.ForeignKey;
import org.dandoy.dbpop.database.Table;
import org.dandoy.dbpop.database.TableName;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.*;

public class ExecutionPlan implements AutoCloseable {
    private final Database database;
    private final File datasetsDirectory;
    private final String dataset;
    private final ExecutionMode executionMode;
    private final ExecutionContext executionContext;
    private final Set<TableName> processed = new HashSet<>();
    private final List<ExecutionNode> executionNodes = new ArrayList<>();

    private ExecutionPlan(Database database, File datasetsDirectory, String dataset, ExecutionMode executionMode, ExecutionContext executionContext) {
        this.database = database;
        this.datasetsDirectory = datasetsDirectory;
        this.dataset = dataset;
        this.executionMode = executionMode;
        this.executionContext = executionContext;
    }

    @Override
    public void close() {
        executionNodes.forEach(ExecutionNode::close);
    }

    private void flush(ExecutionContext executionContext) {
        boolean hasFlushed;
        do {
            hasFlushed = false;
            for (ExecutionNode executionNode : executionNodes) {
                if (executionNode.flush()) {
                    if (!executionContext.keepRunning()) {
                        return;
                    }
                    hasFlushed = true;
                }
            }
        } while (hasFlushed);
    }

    public static Map<TableName, Integer> execute(
            @NotNull Database database,
            @NotNull File datasetsDirectory,
            @NotNull String dataset,
            @NotNull TableName tableName,
            @NotNull TableExecutionModel tableExecutionModel,
            @NotNull List<String> filteredColumns,
            @NotNull Set<List<Object>> pks,
            @NotNull ExecutionMode executionMode,
            @Nullable Integer rowCountLimit
    ) {
        ExecutionContext executionContext = new ExecutionContext(rowCountLimit == null ? Integer.MAX_VALUE : rowCountLimit);
        try (ExecutionPlan executionPlan = new ExecutionPlan(database, datasetsDirectory, dataset, executionMode, executionContext)) {
            ExecutionNode executionNode = executionPlan.build(tableName, tableExecutionModel, filteredColumns);
            executionNode.download(pks);
            executionPlan.flush(executionContext);
            return executionContext.getRowCounts();
        }
    }

    private ExecutionNode build(
            @NotNull TableName tableName,
            @NotNull TableExecutionModel tableExecutionModel,
            @NotNull List<String> filteredColumns
    ) {
        processed.add(tableName);
        Table table = database.getTable(tableName);

        TableDownloader tableDownloader = TableDownloader.builder()
                .setDatabase(database)
                .setDatasetsDirectory(datasetsDirectory)
                .setDataset(dataset)
                .setTableName(table.tableName())
                .setFilteredColumns(filteredColumns)
                .setExecutionMode(executionMode)
                .setExecutionContext(executionContext)
                .build();
        List<SelectedColumn> selectedColumns = tableDownloader.getSelectedColumns();
        List<SelectedColumn> filterSelectedColumns = filteredColumns.stream().map(it -> SelectedColumn.findByName(selectedColumns, it)).toList();
        ExecutionNode executionNode = new ExecutionNode(table.tableName(), tableDownloader, filterSelectedColumns);
        executionNodes.add(executionNode);
        addLookupNodes(tableExecutionModel, executionNode, table);
        addDataNodes(tableExecutionModel, executionNode, table);
        checkConstraintsEmpty(tableExecutionModel, tableName);
        return executionNode;
    }

    private ExecutionNode buildData(TableExecutionModel tableExecutionModel, TableName tableName, List<String> filteredColumns, List<SelectedColumn> pkSelectedColumns) {
        Table table = database.getTable(tableName);

        ExecutionNode executionNode = createExecutionNode(table, filteredColumns, pkSelectedColumns);
        addLookupNodes(tableExecutionModel, executionNode, table);
        addDataNodes(tableExecutionModel, executionNode, table);
        checkConstraintsEmpty(tableExecutionModel, tableName);
        return executionNode;
    }

    private static void checkConstraintsEmpty(TableExecutionModel tableExecutionModel, TableName tableName) {
        if (!tableExecutionModel.constraints().isEmpty()) {
            throw new RuntimeException("Constraints not found: %s:%s".formatted(tableName, tableExecutionModel.constraints().stream().map(TableExecutionModel::constraintName).toList()));
        }
    }

    private ExecutionNode buildLookup(TableExecutionModel tableExecutionModel, TableName tableName, List<String> pkColumns, List<SelectedColumn> fkSelectedColumns) {
        Table table = database.getTable(tableName);

        ExecutionNode executionNode = createExecutionNode(table, pkColumns, fkSelectedColumns);
        addLookupNodes(tableExecutionModel, executionNode, table);
        checkConstraintsEmpty(tableExecutionModel, tableName);

        return executionNode;
    }

    private void addDataNodes(TableExecutionModel parentTableExecutionModel, ExecutionNode parentExecutionNode, Table table) {
        // If we are on "invoices", fetch "invoice_details"
        for (ForeignKey foreignKey : database.getRelatedForeignKeys(table.tableName())) {
            TableExecutionModel tableExecutionModel = parentTableExecutionModel.removeTableExecutionModel(foreignKey.getName());
            if (tableExecutionModel != null) {
                TableName fkTableName = foreignKey.getFkTableName();
                if (processed.add(fkTableName)) {
                    List<String> pkColumns = foreignKey.getPkColumns();
                    List<String> fkColumns = foreignKey.getFkColumns();
                    List<SelectedColumn> pkSelectedColumns = SelectedColumn.findByName(parentExecutionNode.getSelectedColumns(), pkColumns);
                    ExecutionNode childExecutionNode = buildData(tableExecutionModel, fkTableName, fkColumns, pkSelectedColumns);
                    parentExecutionNode.addExecutionNode(childExecutionNode);
                }
            }
        }
    }

    private void addLookupNodes(TableExecutionModel parentTableExecutionModel, ExecutionNode parentExecutionNode, Table table) {
        // If we are on "invoices", fetch "customers"
        for (ForeignKey foreignKey : table.foreignKeys()) {
            TableExecutionModel tableExecutionModel = parentTableExecutionModel.removeTableExecutionModel(foreignKey.getName());
            if (tableExecutionModel != null) {
                TableName pkTableName = foreignKey.getPkTableName();
                if (processed.add(pkTableName)) {
                    List<String> pkColumns = foreignKey.getPkColumns(); // customers.customer_id
                    List<String> fkColumns = foreignKey.getFkColumns(); // invoices.customer_id
                    List<SelectedColumn> fkSelectedColumns = SelectedColumn.findByName(parentExecutionNode.getSelectedColumns(), fkColumns);
                    ExecutionNode childExecutionNode = buildLookup(tableExecutionModel, pkTableName, pkColumns, fkSelectedColumns);
                    parentExecutionNode.addExecutionNode(childExecutionNode);
                }
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
                .setExecutionMode(executionMode)
                .setExecutionContext(executionContext)
                .build();
        ExecutionNode executionNode = new ExecutionNode(table.tableName(), tableDownloader, extractSelectedColumns);
        executionNodes.add(executionNode);
        return executionNode;
    }
}
