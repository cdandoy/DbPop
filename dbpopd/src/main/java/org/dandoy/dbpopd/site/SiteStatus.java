package org.dandoy.dbpopd.site;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public final class SiteStatus {
    private ConnectionStatus sourceConnectionStatus;
    private ConnectionStatus targetConnectionStatus;
    private boolean hasCode;
    private int codeChanges;

    public SiteStatus() {
    }

    public SiteStatus(SiteStatus that) {
        this.sourceConnectionStatus = that.sourceConnectionStatus;
        this.targetConnectionStatus = that.targetConnectionStatus;
        this.hasCode = that.hasCode;
        this.codeChanges = that.codeChanges;
    }

    public void codeChanged() {
        this.codeChanges++;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SiteStatus that)) return false;
        return hasCode == that.hasCode && codeChanges == that.codeChanges;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hasCode, codeChanges);
    }

    public record ConnectionStatus(boolean configured, String errorMessage) {}
}
