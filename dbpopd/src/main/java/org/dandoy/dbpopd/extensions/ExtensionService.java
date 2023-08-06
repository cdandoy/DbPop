package org.dandoy.dbpopd.extensions;

import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.DatabaseCacheService;
import org.dandoy.dbpopd.utils.SqlExecuteUtils;

import java.io.File;
import java.sql.Connection;
import java.util.Arrays;

@Singleton
public class ExtensionService {
    public final ConfigurationService configurationService;
    public final DatabaseCacheService databaseCacheService;

    public ExtensionService(ConfigurationService configurationService, DatabaseCacheService databaseCacheService) {
        this.configurationService = configurationService;
        this.databaseCacheService = databaseCacheService;
    }

    public void afterPopulate() {
        execute("afterPopulate");
    }

    public void afterPopulate(String dataset) {
        execute("afterPopulate/" + dataset);
    }

    private void execute(String path) {
        File directory = new File(configurationService.getExtensionsDirectory(), path);
        File[] files = directory.listFiles();
        if (files != null) {
            Arrays.stream(files)
                    .filter(it -> it.getName().toLowerCase().endsWith(".sql"))
                    .sorted()
                    .forEach(this::execute);
        }
    }

    @SneakyThrows
    private void execute(File file) {
        Database targetDatabase = databaseCacheService.getTargetDatabaseCache();
        Connection connection = targetDatabase.getConnection();
        SqlExecuteUtils.executeSqlFile(connection, file);
    }
}
