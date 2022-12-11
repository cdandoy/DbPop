package org.dandoy.dbpop.utils;

import java.util.ArrayList;
import java.util.List;

public class ExceptionUtils {
    /**
     * Returns the list of error messages in each caused exception.
     * Note that it ignores the suppressed exceptions.
     */
    public static List<String> getErrorMessages(Throwable throwable) {
        return getErrorMessages(throwable, null);
    }

    /**
     * Returns the list of error messages in each caused exception.
     * Note that it ignores the suppressed exceptions.
     */
    public static List<String> getErrorMessages(Throwable throwable, String indentation) {
        List<String> ret = new ArrayList<>();
        String spacer = indentation == null ? "" : " ";
        if (indentation == null) indentation = "";
        StringBuilder indent = new StringBuilder();
        for (; throwable != null; throwable = throwable.getCause()) {
            String message = throwable.getMessage();
            if (message != null) {
                ret.add(indent + spacer + message);
                indent.append(indentation);
            }
        }
        return ret;
    }
}
