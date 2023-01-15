package org.dandoy.dbpop.download;

import org.dandoy.dbpop.database.TableName;

public record TableQuery(TableName tableName, String column, String value) {}
