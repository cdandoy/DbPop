package org.dandoy.diff;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class HtmlGeneratorBuilder {
    private String title = "Diff";
    private String containerClass = "container";
    private String leftClass = "left";
    private String rightClass = "right";
    private String deletedPosClass = "deleted-pos";
    private String deletedLineClass = "deleted-line";
    private String insertedClass = "inserted";
    private String replacedClass = "replaced";
    private String style = """
                <style>
                    .%1$s {
                        display: flex;
                        flex-wrap: wrap;
                        margin-right: -15px;
                        margin-left: -15px;
                        font-family: "Courier New", monospace;
                    }
                    .%2$s, .%3$s {
                        flex-basis: 0;
                        flex-grow: 1;
                        position: relative;
                        width: 100%%;
                        min-height: 1px;
                        padding-right: 15px;
                        padding-left: 15px;
                    }
                    .%4$s {
                        background-color: lightgreen;
                        display: inline-block;
                        min-width: 2px;
                        height: 1em;
                    }
                    .%5$s {
                        border: 1px solid lightgreen;
                    }
                    .%2$s .%6$s {
                        background-color: lightgray;
                    }
                   .%3$s .%6$s {
                        background-color: lightgreen;
                    }
                    .%7$s {
                        background-color: lightblue;
                    }
                </style>
            """;
    private String html = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
             <meta charset="UTF-8">
             <title>%s</title>
             %s
            </head>
            <body>
            <div class="container">
             <div class="left">
            %%s
             </div>
             <div class="right">
            %%s
             </div>
            </div>
            </body>
            </html>
            """;

    public HtmlGenerator build(){
        String template = html.formatted(
                title,
                style.formatted(
                        containerClass,
                        leftClass,
                        rightClass,
                        deletedPosClass,
                        deletedLineClass,
                        insertedClass,
                        replacedClass
                ).replace("%", "%%")
        );
        return new HtmlGenerator(template);
    }
}
