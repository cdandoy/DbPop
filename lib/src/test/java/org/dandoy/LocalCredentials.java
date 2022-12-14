package org.dandoy;

import org.dandoy.dbpop.download.Downloader;
import org.dandoy.dbpop.upload.Populator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record LocalCredentials(String dbUrl, String dbUser, String dbPassword) {

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

        String dbUrl = null;
        String dbUser = null;
        String dbPassword = null;
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
                        case "jdbcurl" -> dbUrl = value;
                        case "username" -> dbUser = value;
                        case "password" -> dbPassword = value;
                    }
                }
            }
        }

        return new LocalCredentials(dbUrl, dbUser, dbPassword);
    }

    public static Populator.Builder mssqlPopulator() {
        return from("mssql").populator();
    }

    public static Downloader.Builder mssqlDownloader() {
        return from("mssql").downloader();
    }

    public static Populator.Builder pgsqlPopulator() {
        return from("pgsql").populator();
    }

    public Populator.Builder populator() {
        return Populator.builder()
                .setDbUrl(dbUrl)
                .setDbUser(dbUser)
                .setDbPassword(dbPassword);
    }

    public Downloader.Builder downloader() {
        return Downloader.builder()
                .setDbUrl(dbUrl)
                .setDbUser(dbUser)
                .setDbPassword(dbPassword);
    }
}
