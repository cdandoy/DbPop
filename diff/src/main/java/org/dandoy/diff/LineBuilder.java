package org.dandoy.diff;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class LineBuilder implements AutoCloseable {
    @Getter
    private final List<List<DiffSegment>> lines;
    private Tag currentTag = null;
    private final StringBuilder currentText = new StringBuilder();
    private List<DiffSegment> currentLine = new ArrayList<>();

    public LineBuilder(List<List<DiffSegment>> lines) {
        this.lines = lines;
    }

    @Override
    public void close() {
        flushSegment();
        flushLine();
    }

    private void flushSegment() {
        if (currentTag != null) {
            DiffSegment diffSegment = new DiffSegment(currentTag, currentText.toString());
            currentLine.add(diffSegment);
            currentText.setLength(0);
        }
    }

    private void flushLine() {
        if (!currentLine.isEmpty()) {
            lines.add(currentLine);
            currentLine = new ArrayList<>();
        }
    }

    public void pushWords(Tag tag, List<String> words) {
        setTag(tag);
        for (String word : words) {
            if ("\n".equals(word)) {
                flushSegment();
                flushLine();
            } else {
                currentText.append(word);
            }
        }
    }

    private void setTag(Tag tag) {
        if (currentTag != null && currentTag != tag) {
            flushSegment();
        }
        currentTag = tag;
    }
}
