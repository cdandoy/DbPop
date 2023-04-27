package org.dandoy.dbpop;

public class FeatureFlags {
    /**
     * Should we create an empty file when downloading an empty table
     */
    public static boolean createCsvFilesForEmptyTables = getDefault("CREATE_CSV_FILES_FOR_EMPTY_TABLES", true);
    /**
     * if createCsvFilesForEmptyTables is true, should we write headers in an empty file
     */
    public static boolean includeCsvHeadersForEmptyTables = getDefault("INCLUDE_CSV_HEADERS_FOR_EMPTY_TABLES", false);

    private static boolean getDefault(String name, @SuppressWarnings("SameParameterValue") boolean defaultValue) {
        String value = System.getenv(name);
        if (value == null) return defaultValue;
        return "1".equals(value) || "true".equalsIgnoreCase(value);
    }
}
