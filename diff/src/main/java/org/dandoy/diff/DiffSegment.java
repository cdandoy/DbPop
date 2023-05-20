package org.dandoy.diff;

public record DiffSegment(int type, String text) {
    public static final int TYPE_KEEP = 0;
    public static final int TYPE_DELETE = 1;
    public static final int TYPE_INSERT = 2;
    public static final int TYPE_REPLACE = 3;

    @Override
    public String toString() {
        return switch (type) {
            case TYPE_KEEP -> "=%s".formatted(text);
            case TYPE_DELETE -> "-";
            case TYPE_INSERT -> "+%s".formatted(text);
            case TYPE_REPLACE -> "X%s".formatted(text);
            default -> throw new RuntimeException("Unexpected type: " + type);
        };
    }

    public static String typeToString(int type){
        return switch (type) {
            case TYPE_KEEP -> "TYPE_KEEP";
            case TYPE_DELETE -> "TYPE_DELETE";
            case TYPE_INSERT -> "TYPE_INSERT";
            case TYPE_REPLACE -> "TYPE_REPLACE";
            default -> throw new RuntimeException("Unexpected type: " + type);
        };
    }
}
