package org.dandoy.diff;

import org.junit.jupiter.api.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

class DiffEngineTest {
    @Test
    void name() {
        String s1 = """
                Quand le  bas et luuud pèse  un couvercle
                Sur l'esprit gémissant en proie aux longs ennuis,
                Et que de l'horizon embrassant tout le cercle
                II nous verse un jour noir plus triste que les nuits;
                Où l'Espérance, comme une chauve-souris,
                S'en va battant les murs de son aile timide
                Et se cognant la tête à des plafonds pourris;
                Quand la pluie étalant ses immenses traînées
                D'une vaste prison imite les barreaux,
                Et qu'un peuple muet d'infâmes araignées
                Vient tendre ses filets au fond de nos cerveaux,
                """;
        String s2 = """
                 le ciel bas et lourd pèse comme un couvercle
                Sur l'esprit gémissant en proie aux longs ennuis,
                Et que de l'horizon embrassant tout le cercle
                II nous verse un jour noir plus triste que les nuits;
                Quand la terre est changée en un cachot humide,
                Où l'Espérance, comme une chauve-souris,
                S'en va battant les murs de son aile timide
                Et se cognant la tête à des plafonds pourris;
                Et qu'un peuple muet d'infâmes araignées
                Vient tendre ses filets au fond de nos cerveaux,
                """;
        ContentDiff contentDiff = new DiffEngine().diff(s1, s2);
        writeFile(contentDiff);
    }

    @Test
    void debug() {
        String s1 = """
                Où l'Espérance, aaa une chauve-souris,
                Quand la pluie étalant ses immenses traînées
                """;
        String s2 = """
                Où l'Espérance, bbb une chauve-souris,
                S'en va battant les murs de son aile timide
                Et se cognant la tête à des plafonds pourris;
                Quand la pluie étalant ses immenses traînées
                """;
        ContentDiff contentDiff = new DiffEngine().diff(s1, s2);
        writeFile(contentDiff);
    }

    @Test
    void testReplace() {
        ContentDiff contentDiff = new DiffEngine().diff(
                "xxx a xxx",
                "xxx b xxx"
        );
        writeFile(contentDiff);
    }

    private static void writeFile(ContentDiff contentDiff) {
        try (Writer writer = new OutputStreamWriter(new FileOutputStream("diff.html"), StandardCharsets.UTF_8)) {
            HtmlGenerator htmlGenerator = HtmlGenerator.builder().build();
            writer.write(htmlGenerator.generate(contentDiff));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}