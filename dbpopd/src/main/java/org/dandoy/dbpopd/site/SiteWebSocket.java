package org.dandoy.dbpopd.site;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.micronaut.jackson.ObjectMapperFactory;
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
    public void onOpen(WebSocketSession session) {
        log.info("SiteWebSocket.onOpen");
    }

    @OnMessage
    public void onMessage(String message, WebSocketSession session) {
        log.debug("SiteWebSocket.onMessage");
    }

    @OnClose
    public void onClose(WebSocketSession session) {
        log.debug("SiteWebSocket.onClose");
    }

    public void sendMessage(Message message) {
        broadcaster.broadcastSync(message);
    }
}
