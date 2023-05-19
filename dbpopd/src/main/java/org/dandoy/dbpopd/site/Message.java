package org.dandoy.dbpopd.site;

import lombok.Getter;

@Getter
public class Message {
    private final MessageType messageType;
    public static final Message CODE_CHANGE_MESSAGE = new Message(MessageType.CODE_CHANGE);

    public Message(MessageType messageType) {
        this.messageType = messageType;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) return true;
        if (!(obj instanceof Message that)) return false;
        return this.messageType.equals(that.messageType);
    }
}
