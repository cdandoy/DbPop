package org.dandoy.dbpop.fs;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("SameParameterValue")
@Slf4j
class SimpleFileSystemTest {
    @Test
    void testDirectory() throws IOException {
        List<SimpleFileSystem> datasets = SimpleFileSystem.fromPath("test_filesystem/").list();
        assertSame(datasets, "test_filesystem/dataset1");

        for (SimpleFileSystem dataset : datasets) {
            List<SimpleFileSystem> databases = dataset.list();
            assertSame(databases, "test_filesystem/dataset1/db1");
            for (SimpleFileSystem database : databases) {
                List<SimpleFileSystem> schemas = database.list();
                assertSame(schemas, "test_filesystem/dataset1/db1/schema1", "test_filesystem/dataset1/db1/schema2");
                SimpleFileSystem schema1 = database.cd("schema1");
                List<SimpleFileSystem> files = schema1.list();
                assertSame(files,
                        "test_filesystem/dataset1/db1/schema1/file1.1.csv",
                        "test_filesystem/dataset1/db1/schema1/file1.2.csv"
                );
                try (InputStream inputStream = files.get(0).createInputStream()) {
                    assertNotNull(inputStream);
                }
                try (InputStream inputStream = files.get(1).createInputStream()) {
                    assertNotNull(inputStream);
                }
            }
        }
        assertNotNull(datasets);
    }

    @Test
    void testJar() throws IOException {
        SimpleFileSystem platform = SimpleFileSystem.fromPath("org/junit/platform");
        List<SimpleFileSystem> platformEntries = platform.list();
        assertContains(platformEntries, "org/junit/platform/engine");
        SimpleFileSystem engine = platform.cd("engine");
        List<SimpleFileSystem> engineEntries = engine.list();
        assertContains(engineEntries,
                "org/junit/platform/engine/discovery",
                "org/junit/platform/engine/DiscoveryFilter.class",
                "org/junit/platform/engine/DiscoverySelector.class"
        );
        try (InputStream inputStream = engine.cd("DiscoveryFilter.class").createInputStream()) {
            assertNotNull(inputStream);
        }
        try (InputStream inputStream = engine.cd("DiscoverySelector.class").createInputStream()) {
            assertNotNull(inputStream);
        }
    }

    private static void assertSame(Collection<SimpleFileSystem> simpleFileSystems, String... paths) {
        List<String> expected = Arrays.stream(paths).sorted().collect(Collectors.toList());
        List<String> actual = simpleFileSystems.stream().map(SimpleFileSystem::getPath).sorted().collect(Collectors.toList());

        assertEquals(expected, actual);
    }

    private static void assertContains(Collection<SimpleFileSystem> simpleFileSystems, String path) {
        assertTrue(simpleFileSystems.stream().anyMatch(sfs -> path.equals(sfs.getPath())));
    }

    private static void assertContains(Collection<SimpleFileSystem> simpleFileSystems, String path1, String... paths) {
        assertContains(simpleFileSystems, path1);
        Arrays.stream(paths).forEach(path -> assertContains(simpleFileSystems, path));
    }
}