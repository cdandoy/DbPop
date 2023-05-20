package org.dandoy.diff;

import java.util.List;
import java.util.stream.Collectors;

public class HtmlGenerator {
    private final String template;

    HtmlGenerator(String template) {
        this.template = template;
    }

    public static HtmlGeneratorBuilder builder() {
        return new HtmlGeneratorBuilder();
    }

    public String generate(ContentDiff contentDiff) {
        return template.formatted(
                generate(contentDiff.leftLines()),
                generate(contentDiff.rightLines())
        );
    }

    private static String generate(List<DiffLine> diffLines) {
        return
                diffLines.stream()
                        .map(HtmlGenerator::generateLines)
                        .collect(Collectors.joining("\n")
                        );
    }

    private static String generateLines(DiffLine diffLine) {
        if (diffLine.segments().size() == 1) {
            DiffSegment diffSegment = diffLine.segments().get(0);
            if (diffSegment.type() == DiffSegment.TYPE_DELETE) {
                return "    <div class='deleted-line'></div>";
            } else {
                return "    <div class=\"%s\">\n%s\n</div>".formatted(
                        switch (diffSegment.type()) {
                            case DiffSegment.TYPE_KEEP -> "keep";
                            case DiffSegment.TYPE_INSERT -> "inserted";
                            default -> throw new RuntimeException("Invalid type " + diffSegment.type());
                        },
                        diffSegment.text()
                );
            }
        } else {
            return "    <div>\n%s\n</div>".formatted(
                    diffLine.segments().stream()
                            .map(HtmlGenerator::generateSpans)
                            .collect(Collectors.joining())
            );
        }
    }

    private static String generateSpans(DiffSegment diffSegment) {
        return switch (diffSegment.type()) {
            case DiffSegment.TYPE_KEEP -> "<span class=\"keep\">%s</span>".formatted(diffSegment.text());
            case DiffSegment.TYPE_DELETE -> "<span class=\"deleted-pos\"> </span>";
            case DiffSegment.TYPE_INSERT -> "<span class=\"inserted\">%s</span>".formatted(diffSegment.text());
            case DiffSegment.TYPE_REPLACE -> "<span class=\"replaced\">%s</span>".formatted(diffSegment.text());
            default -> throw new RuntimeException("Invalid type");
        };
    }
}
