package org.dandoy.dbpopd.datasets;

import java.util.List;

public record DatasetContentResponse(List<DatasetContent> datasetContents, List<TableContent> tableContents) {}
