package org.dandoy.dbpopd.site;

import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.annotation.ServerWebSocket;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
@ServerWebSocket("/ws/site")
public class SiteWebSocket {
    private final WebSocketBroadcaster broadcaster;

    public SiteWebSocket(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    public void sendMessage(String message) {
        broadcaster.broadcastSync(message);
    }
}
