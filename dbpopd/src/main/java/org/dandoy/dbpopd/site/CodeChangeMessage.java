package org.dandoy.dbpopd.site;

public class CodeChangeMessage extends Message {
    public static final CodeChangeMessage MESSAGE = new CodeChangeMessage();

    private CodeChangeMessage() {
        super(MessageType.CODE_CHANGE);
    }
}
