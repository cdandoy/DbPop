package org.dandoy.dbpopd.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DatabasesConfigurationServiceTest {
    @Test
    void name() {
        assertEquals(
                new DatabasesConfigurationService.JdbcUrlComponents("adventure-works", 1433),
                DatabasesConfigurationService.parseJdbcUrl("jdbc:sqlserver://adventure-works")
        );
        assertEquals(
                new DatabasesConfigurationService.JdbcUrlComponents("adventure-works", 111),
                DatabasesConfigurationService.parseJdbcUrl("jdbc:sqlserver://adventure-works:111;database=tempdb;trustServerCertificate=true")
        );
        assertEquals(
                new DatabasesConfigurationService.JdbcUrlComponents("127.0.0.1", 111),
                DatabasesConfigurationService.parseJdbcUrl("jdbc:sqlserver://127.0.0.1:111;database=tempdb;trustServerCertificate=true")
        );
    }
}