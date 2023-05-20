package org.dandoy.diff;

import org.apache.commons.text.diff.EditScript;
import org.apache.commons.text.diff.StringsComparator;

public class DiffEngine {
    public ContentDiff diff(String left, String right) {
        StringsComparator comparator = new StringsComparator(left, right);
        EditScript<Character> script = comparator.getScript();
        DiffCommandVisitor visitor = new DiffCommandVisitor();
        script.visit(visitor);
        visitor.flushLine();
        return new ContentDiff(
                visitor.getLeftLines(),
                visitor.getRightLines()
        );
    }
}
