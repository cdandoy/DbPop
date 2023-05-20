package org.dandoy.diff;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.diff.CommandVisitor;

import java.util.ArrayList;
import java.util.List;

import static org.dandoy.diff.DiffSegment.*;

@Slf4j
class DiffCommandVisitor implements CommandVisitor<Character> {
    private final List<DiffLine> leftLines = new ArrayList<>();
    private final List<DiffLine> rightLines = new ArrayList<>();
    private int currentType = -1;
    private final StringBuilder currentText = new StringBuilder();
    private final List<DiffSegment> leftSegments = new ArrayList<>();
    private final List<DiffSegment> rightSegments = new ArrayList<>();

    @Override
    public void visitDeleteCommand(Character c) {
        if (c == '\n') {
            flushLine();
        } else {
            if (currentType != TYPE_DELETE) {
                flush();
                currentType = TYPE_DELETE;
            }
            currentText.append(c);
        }
    }

    @Override
    public void visitInsertCommand(Character c) {
        if (c == '\n') {
            flushLine();
        } else {
            if (currentType != TYPE_INSERT) {
                flush();
                currentType = TYPE_INSERT;
            }
            currentText.append(c);
        }
    }

    @Override
    public void visitKeepCommand(Character c) {
        if (c == '\n') {
            flushLine();
        } else {
            if (currentType != TYPE_KEEP) {
                flush();
                currentType = TYPE_KEEP;
            }
            currentText.append(c);
        }
    }

    void flushLine() {
        flush();
        appendLine(leftLines, leftSegments);
        appendLine(rightLines, rightSegments);
        currentType = -1;
    }

    private static void appendLine(List<DiffLine> diffLines, List<DiffSegment> diffSegments) {
        // Do not append two consecutive delete lines
        if (diffSegments.size() == 1 && diffSegments.get(0).type() == TYPE_DELETE) {
            if (!diffLines.isEmpty()) {
                DiffLine lastLine = diffLines.get(diffLines.size() - 1);
                if (lastLine.segments().size() == 1) {
                    if (lastLine.segments().get(0).type() == TYPE_DELETE) {
                        diffSegments.clear();
                        return;
                    }
                }
            }
        }
        diffLines.add(new DiffLine(new ArrayList<>(diffSegments)));
        diffSegments.clear();
    }

    private void flush() {
        if (currentType == -1) return;

        log.debug("{} [{}]", typeToString(currentType), currentText);

        switch (currentType) {
            case TYPE_KEEP -> {
                DiffSegment segment = new DiffSegment(TYPE_KEEP, currentText.toString());
                leftSegments.add(segment);
                rightSegments.add(segment);
            }
            case TYPE_DELETE -> {
                if (!leftSegments.isEmpty() && leftSegments.get(leftSegments.size() - 1).type() == TYPE_DELETE) {
                    leftSegments.set(leftSegments.size() - 1, new DiffSegment(TYPE_REPLACE, currentText.toString()));
                    DiffSegment removed = rightSegments.remove(rightSegments.size() - 1);
                    rightSegments.add(new DiffSegment(TYPE_REPLACE, removed.text()));
                } else {
                    leftSegments.add(new DiffSegment(TYPE_INSERT, currentText.toString()));
                    rightSegments.add(new DiffSegment(TYPE_DELETE, ""));
                }
            }
            case TYPE_INSERT -> {
                if (!rightSegments.isEmpty() && rightSegments.get(rightSegments.size() - 1).type() == TYPE_DELETE) {
                    DiffSegment removed = leftSegments.remove(leftSegments.size() - 1);
                    leftSegments.add(new DiffSegment(TYPE_REPLACE, removed.text()));
                    rightSegments.set(rightSegments.size() - 1, new DiffSegment(TYPE_REPLACE, currentText.toString()));
                } else {
                    leftSegments.add(new DiffSegment(TYPE_DELETE, ""));
                    rightSegments.add(new DiffSegment(TYPE_INSERT, currentText.toString()));
                }
            }
        }

        log.debug("left:  {}", leftSegments);
        log.debug("right: {}", rightSegments);
        log.debug("--------------------------------------------------------");

        currentText.setLength(0);
    }

    public List<DiffLine> getLeftLines() {
        return leftLines;
    }

    public List<DiffLine> getRightLines() {
        return rightLines;
    }
}
