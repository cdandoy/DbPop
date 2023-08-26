package org.dandoy.dbpopd.site;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode
public final class SiteStatus {
    /**
     * Status of the source connection.
     */
    @Setter
    private ConnectionStatus sourceConnectionStatus;
    /**
     * Status of the target connection.
     */
    @Setter
    private ConnectionStatus targetConnectionStatus;
    /**
     * True when the code on the file system and in the database has been scanned.
     */
    @Setter
    private boolean codeScanComplete;
    /**
     * True if there are differences between the code on the file system and in the database
     */
    private boolean hasCodeDiffs;
    /**
     * Changed every time hasCodeDiffs is modified so that the Code Changes page can refresh
     */
    private int codeDiffChanges;

    public SiteStatus() {
    }

    public void codeDiffChanged(boolean hasCodeDiffs) {
        this.hasCodeDiffs = hasCodeDiffs;
        this.codeDiffChanges++;
    }

    public record ConnectionStatus(boolean configured, String errorMessage) {}
}
