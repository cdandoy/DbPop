package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import org.dandoy.dbpop.database.ForeignKey;
import org.dandoy.dbpop.database.TableName;

import java.util.Comparator;
import java.util.List;

@Controller("/database")
public class DatabaseVfksController {
    private final ConfigurationService configurationService;

    public DatabaseVfksController(ConfigurationService configurationService) {
        this.configurationService = configurationService;
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
    public void postVirtualForeignKey(ForeignKey foreignKey) {
        configurationService
                .getVirtualFkCache()
                .addFK(foreignKey);
    }

    @Delete("/vfks")
    public void deleteVirtualForeignKey(ForeignKey foreignKey) {
        configurationService
                .getVirtualFkCache()
                .removeFK(foreignKey);
    }
}
