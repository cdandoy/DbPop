package org.dandoy.dbpopd.datasets;

import org.dandoy.dbpop.database.TableName;

import java.util.Map;

public record TableContent(TableName tableName, Map<String, FileContent> content) {}
