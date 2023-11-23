package org.dandoy.dbpopd;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.problem.HttpStatusType;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.dandoy.dbpop.database.*;
import org.dandoy.dbpopd.config.ConfigurationService;
import org.dandoy.dbpopd.config.DatabaseCacheService;
import org.zalando.problem.Problem;

import java.util.Comparator;
import java.util.List;

@Controller("/database")
@Tag(name = "database")
public class DatabaseVfksController {
    private final ConfigurationService configurationService;
    private final DatabaseCacheService databaseCacheService;

    public DatabaseVfksController(ConfigurationService configurationService, DatabaseCacheService databaseCacheService) {
        this.configurationService = configurationService;
        this.databaseCacheService = databaseCacheService;
    }

    @Get("/vfks")
    public List<ForeignKey> getVirtualForeignKeys() {
        return configurationService
                .getVirtualFkCache()
                .getForeignKeys()
                .stream()
                .sorted(
                        Comparator.comparing(it -> it.getFkTableName().toQualifiedName())
                )
                .toList();
    }

    @Get("/vfks/{catalog}/{schema}/{table}/")
    public ForeignKey getVirtualForeignKeys(String catalog, String schema, String table) {
        return configurationService
                .getVirtualFkCache()
                .getByPkTable(new TableName(catalog, schema, table));
    }

    @Get("/vfks/{catalog}/{schema}/{table}/{fkName}")
    public ForeignKey getVirtualForeignKey(String catalog, String schema, String table, String fkName) {
        return configurationService
                .getVirtualFkCache()
                .getByPkTable(
                        new TableName(catalog, schema, table),
                        fkName
                );
    }

    @Post("/vfks")
    public void postVirtualForeignKey(@Body ForeignKey foreignKey) {
        DatabaseCache sourceDatabaseCache = databaseCacheService.getSourceDatabaseCacheOrThrow();
        Table fkTable = sourceDatabaseCache.getTable(foreignKey.getFkTableName());
        List<ForeignKey> dbForeignKeys = fkTable.getForeignKeys();

        VirtualFkCache virtualFkCache = configurationService.getVirtualFkCache();
        ForeignKey existingVfk = virtualFkCache.getByFkTable(foreignKey.getFkTableName(), foreignKey.getName());
        if (existingVfk == null) {
            // We are creating a new VFK
            // Make sure we are not shadowing an existing FK
            dbForeignKeys.stream()
                    .filter(it -> it.getName().equals(foreignKey.getName()) && it.getFkTableName().equals(foreignKey.getFkTableName()))
                    .forEach(existingFk -> {
                        throw Problem.builder()
                                .withStatus(new HttpStatusType(HttpStatus.BAD_REQUEST))
                                .withDetail("Foreign Key with the name %s already exists in the database".formatted(existingFk.getName()))
                                .with("fkName", existingFk.getName())
                                .build();
                    });
        }

        dbForeignKeys.stream()
                .filter(fk -> fk != existingVfk)
                .filter(fk -> fk.getPkTableName().equals(foreignKey.getPkTableName()) &&
                              fk.getFkTableName().equals(foreignKey.getFkTableName()) &&
                              fk.getPkColumns().equals(foreignKey.getFkColumns()))
                .forEach(fk -> {
                    throw Problem.builder()
                            .withStatus(new HttpStatusType(HttpStatus.BAD_REQUEST))
                            .withDetail("Foreign Key already exists: %s".formatted(fk.getName()))
                            .with("fkName", fk.getName())
                            .build();
                });

        configurationService
                .getVirtualFkCache()
                .addFK(foreignKey);
    }

    @Delete("/vfks")
    public void deleteVirtualForeignKey(@Body ForeignKey foreignKey) {
        configurationService
                .getVirtualFkCache()
                .removeFK(foreignKey);
    }
}
