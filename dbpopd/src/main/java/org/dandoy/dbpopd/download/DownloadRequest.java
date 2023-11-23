package org.dandoy.dbpopd.download;

import io.micronaut.core.annotation.Introspected;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.dandoy.dbpop.database.Dependency;

import java.util.Map;

@Getter
@Setter
@Accessors(chain = true)
@Introspected
@Serdeable
public class DownloadRequest {
    @NotNull
    private String dataset;
    @NotNull
    private Dependency dependency;
    private Map<String, String> queryValues;
    private boolean dryRun;
    private int maxRows = 1000;
}
