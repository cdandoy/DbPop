package org.dandoy;

import org.apache.commons.io.IOUtils;
import org.dandoy.dbpop.database.ConnectionBuilder;
import org.dandoy.dbpop.database.UrlConnectionBuilder;
import org.dandoy.dbpop.upload.Populator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record LocalCredentials(ConnectionBuilder sourceConnectionBuilder, ConnectionBuilder targetConnectionBuilder) {

    public static LocalCredentials from(String env) {
        String userHome = System.getProperty("user.home");
        if (userHome == null) throw new RuntimeException("Cannot find your home directory");
        File dir = new File(userHome, ".dbpop");
        if (!dir.isDirectory()) throw new RuntimeException("Directory does not exist: " + dir);
        File propertyFile = new File(dir, "dbpop.properties");
        if (!propertyFile.isFile()) throw new RuntimeException("Property file does not exist: " + propertyFile);

        Properties properties = new Properties();
        try (BufferedReader bufferedReader = Files.newBufferedReader(propertyFile.toPath(), StandardCharsets.UTF_8)) {
            properties.load(bufferedReader);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + propertyFile, e);
        }

        String sourceUrl = null;
        String sourceUser = null;
        String sourcePassword = null;
        String targetUrl = null;
        String targetUser = null;
        String targetPassword = null;
        Pattern splitKey = Pattern.compile("([^.]*)\\.(.*)");
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();
            Matcher matcher = splitKey.matcher(key);
            if (matcher.matches()) {
                String keyEnv = matcher.group(1);
                if (env.equals(keyEnv)) {
                    String keyName = matcher.group(2);
                    String value = entry.getValue().toString();
                    switch (keyName) {
                        case "SOURCE_JDBCURL" -> sourceUrl = value;
                        case "SOURCE_USERNAME" -> sourceUser = value;
                        case "SOURCE_PASSWORD" -> sourcePassword = value;
                        case "TARGET_JDBCURL" -> targetUrl = value;
                        case "TARGET_USERNAME" -> targetUser = value;
                        case "TARGET_PASSWORD" -> targetPassword = value;
                    }
                }
            }
        }

        return new LocalCredentials(
                sourceUrl == null ? null : new UrlConnectionBuilder(sourceUrl, sourceUser, sourcePassword),
                targetUrl == null ? null : new UrlConnectionBuilder(targetUrl, targetUser, targetPassword)
        );
    }

    public static Populator.Builder mssqlPopulator() {
        return from("mssql").populator();
    }

    public static Populator.Builder pgsqlPopulator() {
        return from("pgsql").populator();
    }

    public Connection createTargetConnection() throws SQLException {
        return targetConnectionBuilder.createConnection();
    }

    public Connection createSourceConnection() throws SQLException {
        return sourceConnectionBuilder.createConnection();
    }

    public Populator.Builder populator() {
        return Populator.builder().setConnectionBuilder(targetConnectionBuilder);
    }

    public void executeSource(String... filenames) {
        try (Connection sourceConnection = createSourceConnection()) {
            execute(sourceConnection, filenames);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void executeTarget(String... filenames) {
        try (Connection sourceConnection = createTargetConnection()) {
            execute(sourceConnection, filenames);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void execute(Connection sourceConnection, String... filenames) throws IOException {
        File dir = new File("./src/test/resources/mssql");
        for (String filename : filenames) {
            File file = new File(dir, filename);
            try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
                String sql = IOUtils.toString(reader);
                try (PreparedStatement preparedStatement = sourceConnection.prepareStatement(sql)) {
                    preparedStatement.execute();
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to execute \n%s".formatted(sql), e);
                }
            }
        }
    }
}