package org.dandoy.dbpopd.site;

import lombok.Getter;

@Getter
public class Message {
    private final MessageType messageType;

    public Message(MessageType messageType) {
        this.messageType = messageType;
    }
}
