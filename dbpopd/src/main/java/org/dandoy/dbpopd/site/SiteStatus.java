package org.dandoy.dbpopd.site;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public final class SiteStatus {
    private boolean hasCode;
    private int codeChanges;

    public SiteStatus() {
    }

    public SiteStatus(SiteStatus that) {
        this.hasCode = that.hasCode;
        this.codeChanges = that.codeChanges;
    }

    public void setCodeChanged() {
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
}
