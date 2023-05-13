package org.dandoy.dbpopd.site;

import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.OnClose;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
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

    @OnOpen
    public void onOpen(WebSocketSession ignore) {
        log.debug("SiteWebSocket.onOpen");
    }

    @OnMessage
    public void onMessage(String message, WebSocketSession ignore2) {
        log.debug("SiteWebSocket.onMessage - {}", message);
    }

    @OnClose
    public void onClose(WebSocketSession ignore) {
        log.debug("SiteWebSocket.onClose");
    }

    public void sendMessage(Message message) {
        broadcaster.broadcastSync(message);
    }
}
