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

    public static ExecutionContext execute(
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
        int effectiveRowCountLimit = getEffectiveRowCountLimit(executionMode, rowCountLimit);
        ExecutionContext executionContext = new ExecutionContext(effectiveRowCountLimit);
        try (ExecutionPlan executionPlan = new ExecutionPlan(database, datasetsDirectory, dataset, executionMode, executionContext)) {
            ExecutionNode executionNode = executionPlan.build(tableName, tableExecutionModel, filteredColumns);
            executionNode.download(pks);
            executionPlan.flush(executionContext);
            return executionContext;
        }
    }

    private static int getEffectiveRowCountLimit(ExecutionMode executionMode, Integer rowCountLimit) {
        if (executionMode == ExecutionMode.COUNT) {
            if (rowCountLimit != null) return rowCountLimit;
        }
        return Integer.MAX_VALUE;
    }

    private ExecutionNode build(
            @NotNull TableName tableName,
            @NotNull TableExecutionModel tableExecutionModel,
            @NotNull List<String> filteredColumns
    ) {
        processed.add(tableName);
        Table table = database.getTable(tableName);

        List<TableJoin> tableJoins = new ArrayList<>();
        List<TableQuery> wheres = new ArrayList<>();
        collectJoins(database, tableName, tableExecutionModel, tableJoins, wheres);

        TableDownloader tableDownloader = TableDownloader.builder()
                .setDatabase(database)
                .setDatasetsDirectory(datasetsDirectory)
                .setDataset(dataset)
                .setTableName(table.tableName())
                .setTableJoins(tableJoins)
                .setWheres(wheres)
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
        executionContext.tableAdded(tableName);
        return executionNode;
    }

    private static boolean collectJoins(Database database, TableName tableName, TableExecutionModel tableExecutionModel, List<TableJoin> tableJoins, List<TableQuery> where) {
        Table table = database.getTable(tableName);
        for (TableExecutionModel subModel : tableExecutionModel.constraints()) {
            Optional<ForeignKey> optionalForeignKey = table.foreignKeys().stream()
                    .filter(it -> subModel.constraintName().equals(it.getName()))
                    .findFirst();
            if (optionalForeignKey.isPresent()) {
                ForeignKey foreignKey = optionalForeignKey.get();
                boolean hasJoins = collectJoins(database, foreignKey.getPkTableName(), subModel, tableJoins, where);
                if (hasJoins) {
                    List<TableCondition> onClauses = new ArrayList<>();
                    List<String> pkColumns = foreignKey.getPkColumns();
                    List<String> fkColumns = foreignKey.getFkColumns();
                    for (int i = 0; i < pkColumns.size(); i++) {
                        onClauses.add(
                                new TableCondition(
                                        pkColumns.get(i),
                                        fkColumns.get(i)
                                )
                        );
                    }
                    tableJoins.add(
                            new TableJoin(
                                    foreignKey.getPkTableName(),
                                    foreignKey.getFkTableName(),
                                    onClauses

                            )
                    );
                }
            }
        }

        if (tableExecutionModel.queries().isEmpty()) return false;

        tableExecutionModel.queries().forEach(query -> {
            TableQuery tableQuery = new TableQuery(tableName, query.column(), query.value());
            where.add(tableQuery);
        });
        return true;
    }

    private ExecutionNode buildData(TableExecutionModel tableExecutionModel, TableName tableName, List<String> filteredColumns, List<SelectedColumn> pkSelectedColumns) {
        Table table = database.getTable(tableName);

        ExecutionNode executionNode = createExecutionNode(table, tableExecutionModel, filteredColumns, pkSelectedColumns);
        addLookupNodes(tableExecutionModel, executionNode, table);
        addDataNodes(tableExecutionModel, executionNode, table);
        checkConstraintsEmpty(tableExecutionModel, tableName);
        executionContext.tableAdded(tableName);
        return executionNode;
    }

    private static void checkConstraintsEmpty(TableExecutionModel tableExecutionModel, TableName tableName) {
        if (!tableExecutionModel.constraints().isEmpty()) {
            throw new RuntimeException("Constraints not found: %s:%s".formatted(tableName, tableExecutionModel.constraints().stream().map(TableExecutionModel::constraintName).toList()));
        }
    }

    private ExecutionNode buildLookup(TableExecutionModel tableExecutionModel, TableName tableName, List<String> pkColumns, List<SelectedColumn> fkSelectedColumns) {
        Table table = database.getTable(tableName);

        ExecutionNode executionNode = createExecutionNode(table, tableExecutionModel, pkColumns, fkSelectedColumns);
        addLookupNodes(tableExecutionModel, executionNode, table);
        checkConstraintsEmpty(tableExecutionModel, tableName);
        executionContext.tableAdded(tableName);

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
    private ExecutionNode createExecutionNode(Table table, TableExecutionModel tableExecutionModel, List<String> filteredColumns, List<SelectedColumn> extractSelectedColumns) {
        List<TableJoin> tableJoins = new ArrayList<>();
        List<TableQuery> wheres = new ArrayList<>();
        collectJoins(database, table.tableName(), tableExecutionModel, tableJoins, wheres);
        TableDownloader tableDownloader = TableDownloader.builder()
                .setDatabase(database)
                .setDatasetsDirectory(datasetsDirectory)
                .setDataset(dataset)
                .setTableName(table.tableName())
                .setTableJoins(tableJoins)
                .setWheres(wheres)
                .setFilteredColumns(filteredColumns)
                .setExecutionMode(executionMode)
                .setExecutionContext(executionContext)
                .build();
        ExecutionNode executionNode = new ExecutionNode(table.tableName(), tableDownloader, extractSelectedColumns);
        executionNodes.add(executionNode);
        return executionNode;
    }
}
