package org.dandoy.dbpopd.site;

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.OnClose;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
import io.micronaut.websocket.annotation.ServerWebSocket;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpopd.config.ConnectionBuilderChangedEvent;
import org.dandoy.dbpopd.config.ConnectionType;

import java.util.function.Supplier;

@Slf4j
@Singleton
@ServerWebSocket("/ws/site")
public class SiteWebSocket implements ApplicationEventListener<ConnectionBuilderChangedEvent> {
    private final WebSocketBroadcaster broadcaster;
    @Getter
    private final SiteStatus siteStatus = new SiteStatus();
    private SiteStatus siteStatusBeforeDelay;
    private int delayed = 0;

    public SiteWebSocket(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @OnOpen
    public void onOpen(WebSocketSession ignore) {
        sendSiteStatus();
    }

    @OnMessage
    public void onMessage(String message, WebSocketSession ignore2) {
        log.debug("SiteWebSocket.onMessage - {}", message);
    }

    @OnClose
    public void onClose(WebSocketSession ignore) {
        log.debug("SiteWebSocket.onClose");
    }

    @Override
    public void onApplicationEvent(ConnectionBuilderChangedEvent event) {
        SiteStatus.ConnectionStatus connectionStatus = new SiteStatus.ConnectionStatus(event.isConfigured(), event.errorMessage());
        if (event.type() == ConnectionType.SOURCE) {
            siteStatus.setSourceConnectionStatus(connectionStatus);
        } else if (event.type() == ConnectionType.TARGET) {
            siteStatus.setTargetConnectionStatus(connectionStatus);
        }
        fireSiteStatus();
    }

    public void setHasCode(boolean hasCode) {
        siteStatus.setHasCode(hasCode);
        fireSiteStatus();
    }

    public void codeChanged() {
        siteStatus.codeChanged();
        fireSiteStatus();
    }

    private void fireSiteStatus() {
        if (delayed == 0) {
            sendSiteStatus();
        }
    }

    private void sendSiteStatus() {
        broadcaster.broadcastSync(siteStatus);
    }

    public <T> T holdChanges(Supplier<T> supplier) {
        setDelayed(true);
        try {
            return supplier.get();
        } finally {
            setDelayed(false);
        }
    }

    private synchronized void setDelayed(boolean delayed) {
        if (this.delayed == 0) {
            siteStatusBeforeDelay = new SiteStatus(siteStatus);
        }
        this.delayed += delayed ? 1 : -1;
        if (this.delayed == 0) {
            if (!siteStatus.equals(siteStatusBeforeDelay)) {
                sendSiteStatus();
            }
        } else if (this.delayed < 0 || this.delayed > 3) {
            throw new RuntimeException("Unexpected delayed value: " + this.delayed);
        }
    }
}
