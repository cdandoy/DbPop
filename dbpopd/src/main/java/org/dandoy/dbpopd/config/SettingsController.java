package org.dandoy.dbpopd.config;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.problem.HttpStatusType;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.zalando.problem.Problem;

import java.sql.Connection;
import java.sql.SQLException;

@Controller("/settings")
@Slf4j
public class SettingsController {
    private final DatabasesConfigurationService databasesConfigurationService;

    public SettingsController(DatabasesConfigurationService databasesConfigurationService) {
        this.databasesConfigurationService = databasesConfigurationService;
    }

    @Get
    public Settings getSettings() {
        DatabaseConfiguration[] databaseConfigurations = databasesConfigurationService.getDatabaseConfigurations();
        return new Settings(
                toDatabaseConfigurationResponse(databaseConfigurations, ConnectionType.SOURCE),
                toDatabaseConfigurationResponse(databaseConfigurations, ConnectionType.TARGET)
        );
    }

    @Post("/source")
    public void postSource(@Body DatabaseConfigurationResponse databaseConfigurationBody) {
        saveDatabaseConfiguration(ConnectionType.SOURCE, databaseConfigurationBody);
    }

    @Post("/target")
    public void postTarget(@Body DatabaseConfigurationResponse databaseConfigurationBody) {
        saveDatabaseConfiguration(ConnectionType.TARGET, databaseConfigurationBody);
    }

    private void saveDatabaseConfiguration(ConnectionType connectionType, DatabaseConfigurationResponse body) {
        DatabaseConfiguration oldDatabaseConfiguration = databasesConfigurationService.getDatabaseConfiguration(connectionType);
        DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration(
                body.disabled(),
                body.url(),
                body.username(),
                body.password().equals("*****") ? oldDatabaseConfiguration.password() : body.password()
        );
        databasesConfigurationService.setDatabaseConfiguration(connectionType, databaseConfiguration);
        if (!body.disabled()) {
            validateDatabaseConfiguration(databaseConfiguration);
        }
    }

    private void validateDatabaseConfiguration(DatabaseConfiguration databaseConfiguration) {
        if (!databaseConfiguration.hasInfo()) return;
        UrlConnectionBuilder urlConnectionBuilder = new UrlConnectionBuilder(
                databaseConfiguration.url(),
                databaseConfiguration.username(),
                databaseConfiguration.password()
        );
        //noinspection EmptyTryBlock
        try (Connection ignored = urlConnectionBuilder.createConnection()) {
        } catch (SQLException e) {
            throw Problem.builder()
                    .withStatus(new HttpStatusType(HttpStatus.BAD_REQUEST))
                    .withDetail(e.getMessage())
                    .build();
        }
    }

    private static DatabaseConfigurationResponse toDatabaseConfigurationResponse(DatabaseConfiguration[] databaseConfigurations, ConnectionType type) {
        DatabaseConfiguration databaseConfiguration = databaseConfigurations[type.ordinal()];
        return new DatabaseConfigurationResponse(
                databaseConfiguration.disabled(),
                databaseConfiguration.url(),
                databaseConfiguration.username(),
                null,
                databaseConfiguration.conflict()
        );
    }

    public record DatabaseConfigurationResponse(boolean disabled, String url, String username, String password, boolean conflict) {}

    public record Settings(DatabaseConfigurationResponse sourceDatabaseConfiguration, DatabaseConfigurationResponse targetDatabaseConfiguration) {}
}
