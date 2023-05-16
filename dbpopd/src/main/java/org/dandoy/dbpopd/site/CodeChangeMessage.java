package org.dandoy.dbpopd.site;

public class CodeChangeMessage extends Message {
    public static final CodeChangeMessage MESSAGE = new CodeChangeMessage();

    private CodeChangeMessage() {
        super(MessageType.CODE_CHANGE);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object obj) {
        return this == MESSAGE;
    }
}
