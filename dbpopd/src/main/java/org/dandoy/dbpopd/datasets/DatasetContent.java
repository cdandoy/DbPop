package org.dandoy.dbpopd.datasets;

import java.util.List;

public record DatasetContent(String name, int fileCount, long size, int rows, boolean active, Integer loadedRows, Long executionTime, List<String> failureCauses) {}
