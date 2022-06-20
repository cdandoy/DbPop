package org.dandoy.dbpop;

class Settings {
    /**
     * Disable foreign keys before inserts vs. drop constraints, and indexes
     */
    static final boolean DISABLE_CONTRAINTS = true;
    /**
     * Check the integrity of the data when re-enabling the foreign keys
     */
    static final boolean CHECK_CONTRAINTS = false;
}
