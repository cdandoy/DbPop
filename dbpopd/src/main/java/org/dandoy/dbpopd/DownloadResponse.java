package org.dandoy.dbpopd;

import org.dandoy.dbpop.database.TableName;

import java.util.Map;

public record DownloadResponse(Map<TableName, Integer> rowCounts) {
}
