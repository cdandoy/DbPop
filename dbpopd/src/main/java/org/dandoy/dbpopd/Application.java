package org.dandoy.dbpopd;

import io.micronaut.runtime.Micronaut;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

@OpenAPIDefinition(
        info = @Info(
                title = "dbpopd",
                version = "0.1.5"
        )
)
public class Application {

    @Getter
    private static long startTimeMilis;

    public static void main(String[] args) {
        startTimeMilis = System.currentTimeMillis();
        System.out.println("""
                 _____  _     _____
                |  __ \\| |   |  __ \\
                | |  | | |__ | |__) |__  _ __
                | |  | | '_ \\|  ___/ _ \\| '_ \\
                | |__| | |_) | |  | (_) | |_) |
                |_____/|_.__/|_|   \\___/| .__/
                                        | |
                                        |_|""");
        String version = getVersion();
        if (version != null) {
            System.out.printf(" DbPop (v%s)\n%n", version);
        }

        Micronaut.build(args)
                .banner(false)
                .deduceEnvironment(false)
                .start();
    }

    @SneakyThrows
    private static String getVersion() {
        Properties properties = new Properties();
        try (InputStream inputStream = Application.class.getResourceAsStream("/version.txt")) {
            if (inputStream != null) {
                try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                    properties.load(bufferedReader);
                }
            }
        }
        return properties.getProperty("app.version");
    }
}
