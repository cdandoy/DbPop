package org.dandoy.dbpop.database;

/**
 * A DatabasePreparationStrategy prepares the tables for deletion and insertion.
 * The prefered strategy for SQL Server is to just disable and re-enable the foreign keys.
 * Code to execute before the deletes happens during the construction, code to execute after the inserts are in the {@link #close()} method.
 */
public abstract class DatabasePreparationStrategy implements AutoCloseable {

    @Override
    public abstract void close();
}
