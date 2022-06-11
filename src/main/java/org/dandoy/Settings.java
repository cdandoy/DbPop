package org.dandoy;

public class Settings {
    /**
     * Disable foreign keys before inserts vs. drop constraints, and indexes
     */
    public static final boolean DISABLE_CONTRAINTS = true;
    /**
     * Check the integrity of the data when re-enabling the foreign keys
     */
    public static final boolean CHECK_CONTRAINTS = false;
}
