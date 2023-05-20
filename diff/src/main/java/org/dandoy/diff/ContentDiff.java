package org.dandoy.diff;

import java.util.List;

public record ContentDiff(List<DiffLine> leftLines, List<DiffLine> rightLines) {}
