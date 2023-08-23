package org.dandoy.dbpopd.site;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode
public final class SiteStatus {
    @Setter
    private ConnectionStatus sourceConnectionStatus;
    @Setter
    private ConnectionStatus targetConnectionStatus;
    private boolean hasCodeDiffs;
    private int codeDiffChanges;

    public SiteStatus() {
    }

    public void codeDiffChanged(boolean hasCodeDiffs) {
        this.hasCodeDiffs = hasCodeDiffs;
        this.codeDiffChanges++;
    }

    public record ConnectionStatus(boolean configured, String errorMessage) {}
}
