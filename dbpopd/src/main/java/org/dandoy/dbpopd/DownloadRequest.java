package org.dandoy.dbpopd;

import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.validation.constraints.NotNull;

@Getter
@Setter
@Accessors(chain = true)
@Introspected
public class DownloadRequest {
    @NotNull
    private String dataset;
    private String catalog;
    private String schema;
}
