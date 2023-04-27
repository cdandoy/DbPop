package org.dandoy.dbpop.utils;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class MultiCauseException extends RuntimeException {
    @Getter
    private final List<String> causes;

    public MultiCauseException(List<String> causes) {
        this.causes = causes;
    }

    public MultiCauseException(String message, Exception e) {
        super(message, e);
        causes = getCauses(e);
        causes.add(0, message);
    }

    public static MultiCauseException build(Throwable throwable) {
        return new MultiCauseException(getCauses(throwable));
    }

    public static List<String> getCauses(Throwable throwable) {
        List<String> ret = new ArrayList<>();
        for (; throwable != null; throwable = throwable.getCause()) {
            if (throwable instanceof MultiCauseException multiCauseException) {
                ret.addAll(multiCauseException.causes);
            } else {
                String message = throwable.getMessage();
                if (message != null) {
                    ret.add(message);
                }
            }
        }
        return ret;
    }
}
