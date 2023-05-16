package org.dandoy.dbpopd.site;

import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.OnClose;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
import io.micronaut.websocket.annotation.ServerWebSocket;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Singleton
@ServerWebSocket("/ws/site")
public class SiteWebSocket {
    private final WebSocketBroadcaster broadcaster;
    private final List<Message> delayedMessages = new ArrayList<>();
    private int delayed = 0;

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

    public synchronized void sendMessage(Message message) {
        if (delayed == 0) {
            broadcaster.broadcastSync(message);
        } else {
            delayedMessages.add(message);
        }
    }

    public void holdChanges(Runnable runnable) {
        setDelayed(true);
        try {
            runnable.run();
        } finally {
            setDelayed(false);
        }
    }

    private synchronized void setDelayed(boolean delayed) {
        this.delayed += delayed ? 1 : -1;
        if (this.delayed == 0) {
            delayedMessages.stream().distinct().forEach(this::sendMessage);
            delayedMessages.clear();
        } else if (this.delayed < 0 || this.delayed > 3) {
            throw new RuntimeException("Unexpected delayed value: " + this.delayed);
        }
    }
}
