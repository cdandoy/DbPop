package org.dandoy.dbpopd.config;

import org.dandoy.dbpop.database.DatabaseCache;

public record DatabaseCacheChangedEvent(ConnectionType type, DatabaseCache databaseCache) {}
