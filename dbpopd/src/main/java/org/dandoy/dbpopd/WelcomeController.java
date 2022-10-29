package org.dandoy.dbpopd;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

@Controller
public class WelcomeController {
    @Get(produces = "text/html")
    public String welcome() {
        //noinspection HtmlUnknownTarget
        return """
                <html lang="en">
                <head>
                    <title>dbpop</title>
                </head>
                <body>
                <div>Welcome to dbpop</div>
                <div>
                    <a href='swagger-ui/'>API documentation</a>
                </div>
                </body>
                </html>
                """;
    }
}