package org.dandoy.dbpopd.site;

import io.micronaut.serde.annotation.Serdeable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode
@Serdeable
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

    public SiteStatus() {
    }

    @Serdeable
    public record ConnectionStatus(boolean configured, String errorMessage) {}
}
