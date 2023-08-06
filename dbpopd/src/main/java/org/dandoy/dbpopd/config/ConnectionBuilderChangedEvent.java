package org.dandoy.dbpopd.config;

import org.dandoy.dbpop.database.ConnectionBuilder;

public record ConnectionBuilderChangedEvent(ConnectionType type, ConnectionBuilder connectionBuilder) {}