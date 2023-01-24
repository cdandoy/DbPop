package org.dandoy.dbpopd.download;

import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.dandoy.dbpop.database.Dependency;

import javax.validation.constraints.NotNull;
import java.util.Map;

@Getter
@Setter
@Accessors(chain = true)
@Introspected
public class DownloadRequest {
    @NotNull
    private String dataset;
    @NotNull
    private Dependency dependency;
    private Map<String, String> queryValues;
    private boolean dryRun;
    private int maxRows = 1000;
}
