package org.dandoy.diff;

import java.util.List;

public record DiffLine(List<DiffSegment> segments) {}