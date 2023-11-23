package org.dandoy.dbpopd.datasets;

import io.micronaut.serde.annotation.Serdeable;
import org.dandoy.dbpop.database.TableName;

import java.util.Map;

@Serdeable
public record TableContent(TableName tableName, Map<String, FileContent> content) {}
