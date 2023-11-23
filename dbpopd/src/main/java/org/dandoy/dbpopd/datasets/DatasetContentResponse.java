package org.dandoy.dbpopd.datasets;

import io.micronaut.serde.annotation.Serdeable;

import java.util.List;

@Serdeable
public record DatasetContentResponse(List<DatasetContent> datasetContents, List<TableContent> tableContents) {}
