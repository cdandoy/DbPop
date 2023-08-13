package org.dandoy.dbpopd.config;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.problem.HttpStatusType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.zalando.problem.Problem;

import java.sql.Connection;
import java.sql.SQLException;

@Controller("/settings")
@Slf4j
public class SettingsController {
    public static final String FAKE_PASSWORD = "*****";
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
        String url = body.url();
        String username = body.username();
        String password = extractPasswordFromRequest(
                databasesConfigurationService.getDatabaseConfiguration(connectionType).password(),
                body.password()
        );
        databasesConfigurationService.setDatabaseConfiguration(connectionType, url, username, password);
        if (!StringUtils.isBlank(url)) {
            validateDatabaseConfiguration(url, username, password);
        }
    }

    private static String extractPasswordFromRequest(String oldPassword, String newPassword) {
        if (newPassword == null) return null;
        if (newPassword.equals(FAKE_PASSWORD)) return oldPassword;
        return newPassword;
    }

    private void validateDatabaseConfiguration(String url, String username, String password) {
        if (StringUtils.isBlank(url)) return;
        UrlConnectionBuilder urlConnectionBuilder = new UrlConnectionBuilder(url, username, password);
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
                databaseConfiguration.url(),
                databaseConfiguration.username(),
                StringUtils.isBlank(databaseConfiguration.password()) ? null : FAKE_PASSWORD,
                databaseConfiguration.fromEnvVariables()
        );
    }

    public record DatabaseConfigurationResponse(String url, String username, String password, boolean fromEnvVariables) {}

    public record Settings(DatabaseConfigurationResponse sourceDatabaseConfiguration, DatabaseConfigurationResponse targetDatabaseConfiguration) {}
}
