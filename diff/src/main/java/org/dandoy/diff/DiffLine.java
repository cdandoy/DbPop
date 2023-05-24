package org.dandoy.diff;

import java.util.List;

public record DiffLine(Tag tag, List<DiffSegment> leftSegments, List<DiffSegment> rightSegments) {}