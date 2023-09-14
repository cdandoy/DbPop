package org.dandoy.dbpopd.config;

import org.dandoy.dbpop.database.DatabaseVersion;

public record DatabaseVersionChangedEvent(ConnectionType type, DatabaseVersion databaseVersion) {}
