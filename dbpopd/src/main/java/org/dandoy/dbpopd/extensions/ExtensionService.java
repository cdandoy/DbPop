package org.dandoy.dbpopd.extensions;

import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import org.dandoy.dbpop.database.Database;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.utils.SqlExecuteUtils;

import java.io.File;
import java.sql.Connection;
import java.util.Arrays;

@Singleton
public class ExtensionService {
    public final ConfigurationService configurationService;

    public ExtensionService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
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
        Database targetDatabase = configurationService.getTargetDatabaseCache();
        Connection connection = targetDatabase.getConnection();
        SqlExecuteUtils.executeSqlFile(connection, file);
    }
}
