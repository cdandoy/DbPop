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

    public StartupListener(@Nullable EmbeddedServer embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

    @EventListener
    public void onStartupEvent(StartupEvent event) {
        long started = System.currentTimeMillis();
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
