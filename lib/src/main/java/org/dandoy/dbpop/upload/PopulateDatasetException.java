package org.dandoy.dbpop.upload;

import lombok.Getter;

public class PopulateDatasetException extends RuntimeException {
    @Getter
    private final String dataset;

    public PopulateDatasetException(String dataset, String message, Exception cause) {
        super(message, cause);
        this.dataset = dataset;
    }
}
