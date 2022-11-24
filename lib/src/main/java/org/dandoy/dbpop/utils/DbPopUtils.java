package org.dandoy.dbpop.utils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DbPopUtils {

    /**
     * Returns the position (starting at 0) of the column name in metaData or -1 if it does not exist.
     */
    public static int getPositionByColumnName(ResultSetMetaData metaData, String columnName) {
        try {
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                if (columnName.equals(metaData.getColumnName(i + 1))) {
                    return i;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return -1;
    }
}
