package org.dandoy.dbpopd;

import io.micronaut.context.event.StartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.runtime.server.EmbeddedServer;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Singleton
@Slf4j
public class StartupListener {
    private final EmbeddedServer embeddedServer;
    private final ConfigurationService configurationService;

    public StartupListener(@Nullable EmbeddedServer embeddedServer, ConfigurationService configurationService) {
        this.embeddedServer = embeddedServer;
        this.configurationService = configurationService;
    }

    @EventListener
    public void onStartupEvent(StartupEvent event) {
        long started = System.currentTimeMillis();
        log.info("Running in \"{}\" mode", configurationService.getMode());
        if (embeddedServer != null) {
            log.info(
                    "Startup completed in {}ms. Server Running: http://localhost:{}/",
                    started - Application.getStartTimeMilis(),
                    embeddedServer.getPort()
            );
        } else {
            log.info(
                    "Startup completed in {}ms.",
                    started - Application.getStartTimeMilis()
            );
        }
    }
}
