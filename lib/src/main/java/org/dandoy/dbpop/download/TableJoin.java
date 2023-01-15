package org.dandoy.dbpop.download;

import org.dandoy.dbpop.database.TableName;

import java.util.List;

public record TableJoin(TableName leftTable, TableName rightTable, List<TableCondition> tableConditions) {}
