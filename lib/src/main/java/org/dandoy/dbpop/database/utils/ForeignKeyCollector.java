package org.dandoy.dbpop.database.utils;

import java.util.ArrayList;
import java.util.List;

public class ForeignKeyCollector implements AutoCloseable {
    private final ForeignKeyConsumer foreignKeyConsumer;
    private String lastConstraint;
    private String lastConstraintDef;
    private String lastFkSchema;
    private String lastFkTable;
    private List<String> fkColumns;
    private String lastPkSchema;
    private String lastPkTable;
    private List<String> pkColumns;

    public ForeignKeyCollector(ForeignKeyConsumer foreignKeyConsumer) {
        this.foreignKeyConsumer = foreignKeyConsumer;
    }

    @Override
    public void close() {
        flush();
    }

    public void push(String constraint, String constraintDef, String fkSchema, String fkTable, String fkColumn, String pkSchema, String pkTable, String pkColumn) {
        if (!(constraint.equals(lastConstraint) && fkSchema.equals(lastFkSchema) && fkTable.equals(lastFkTable))) {
            flush();
            lastConstraint = constraint;
            lastConstraintDef = constraintDef;
            lastFkSchema = fkSchema;
            lastFkTable = fkTable;
            fkColumns = new ArrayList<>();
            lastPkSchema = pkSchema;
            lastPkTable = pkTable;
            pkColumns = new ArrayList<>();
        }
        fkColumns.add(fkColumn);
        pkColumns.add(pkColumn);
    }

    private void flush() {
        if (fkColumns != null) {
            foreignKeyConsumer.consume(lastConstraint, lastConstraintDef, lastFkSchema, lastFkTable, fkColumns, lastPkSchema, lastPkTable, pkColumns);
        }
    }

    public interface ForeignKeyConsumer {
        void consume(String constraint, String constraintDef, String fkSchema, String fkTable, List<String> fkColumns, String pkSchema, String pkTable, List<String> pkColumns);
    }
}
