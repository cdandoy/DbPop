package org.dandoy.dbpop.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Env {
    private final String environment;
    private final Map<String, Environment> environments;

    private Env(String environment, Map<String, Environment> environments) {
        this.environments = environments;
        this.environment = environment;
    }

    public Env environment(String environment) {
        if (environment == null) environment = "default";
        if (environment.equals(this.environment)) return this;
        return new Env(environment, environments);
    }

    public String getString(String key) {
        return getString(key, null);
    }

    public String getString(String key, String defaultValue) {
        return environments.get(environment).getOrDefault(key, defaultValue);
    }

    public static Env createEnv() {
        String userHome = System.getProperty("user.home");
        if (userHome == null) throw new RuntimeException("Cannot find your home directory");
        File dir = new File(userHome, ".dbpop");
        if (dir.isDirectory()) {
            File propertyFile = new File(dir, "dbpop.properties");
            if (propertyFile.exists()) return loadEnvironmentFromProperties(propertyFile);
            return null;
        }
        return null;
    }

    public static Env createEnv(File dir) {
        File propertyFile = new File(dir, "dbpop.properties");
        if (propertyFile.exists()) return loadEnvironmentFromProperties(propertyFile);
        return null;
    }

    private static Env loadEnvironmentFromProperties(File propertyFile) {
        Map<String, Environment> map = new HashMap<>();
        Pattern splitKey = Pattern.compile("([^.]*)\\.(.*)");
        Properties properties = new Properties();
        try (BufferedReader bufferedReader = Files.newBufferedReader(propertyFile.toPath(), StandardCharsets.UTF_8)) {
            properties.load(bufferedReader);
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                String env = "default";
                String key = entry.getKey().toString();
                String value = entry.getValue().toString();
                Matcher matcher = splitKey.matcher(key);
                if (matcher.matches()) {
                    env = matcher.group(1);
                    key = matcher.group(2);
                }
                map.computeIfAbsent(env, s -> new Environment())
                        .put(key, value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Env("default", map);
    }

    static class Environment extends HashMap<String, String> {
    }
}
