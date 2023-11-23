package org.dandoy.dbpopd.datasets;

import io.micronaut.serde.annotation.Serdeable;

import java.util.List;

@Serdeable
public record DatasetContent(String name, int fileCount, long size, int rows, boolean active, Integer loadedRows, Long executionTime, List<String> failureCauses) {}
