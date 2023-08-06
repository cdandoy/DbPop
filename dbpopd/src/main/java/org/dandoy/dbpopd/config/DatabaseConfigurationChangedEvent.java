package org.dandoy.dbpopd.config;

public record DatabaseConfigurationChangedEvent(
        ConnectionType type,
        DatabaseConfiguration databaseConfiguration
) {}