package org.dandoy.diff;

public record DiffSegment(Tag tag, String text) {
    public DiffSegment(String text) {
        this(Tag.EQUAL, text);
    }
}