package org.dandoy.dbpop.fs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class SimpleFileSystem implements Comparable<SimpleFileSystem> {
    private final String path;

    protected SimpleFileSystem(String path) {
        this.path = path;
    }

    public static SimpleFileSystem fromPath(String path) {
        return new SimpleFileSystem(trimSlashes(path));
    }

    static String trimSlashes(String path) {
        if (path.charAt(0) == '/') {
            path = path.substring(1);
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    @Override
    public String toString() {
        return path;
    }

    public String getPath() {
        return path;
    }

    public SimpleFileSystem cd(String relativePath) {
        return new SimpleFileSystem(this.path + "/" + trimSlashes(relativePath));
    }

    public List<SimpleFileSystem> list() {
        Set<SimpleFileSystem> ret = new TreeSet<>();
        for (ClassLoader classLoader = getClass().getClassLoader();
             classLoader != null;
             classLoader = classLoader.getParent()) {
            if (classLoader instanceof URLClassLoader) {
                URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
                URL[] urls = urlClassLoader.getURLs();
                if (urls != null) {
                    for (URL url : urls) {
                        if ("file".equals(url.getProtocol())) {
                            File file = toFile(url);
                            if (file.isFile()) {
                                String fileName = file.getName().toLowerCase();
                                if (fileName.endsWith(".jar") || fileName.endsWith(".zip")) {
                                    listZip(ret, file);
                                }
                            } else {
                                listFiles(ret, file);
                            }
                        }
                    }
                }
            }
        }
        return new ArrayList<>(ret);
    }

    void listZip(Collection<SimpleFileSystem> ret, File file) {
        try {
            String search = path + "/";
            try (ZipFile zipFile = new ZipFile(file)) {
                Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    ZipEntry zipEntry = entries.nextElement();
                    String zipEntryName = trimSlashes(zipEntry.getName());
                    if (zipEntryName.startsWith(search)) {
                        String subPath = zipEntryName.substring(search.length());
                        subPath = trimSlashes(subPath);
                        if (!subPath.contains("/")) {
                            ret.add(new ZipFileSystem(file, zipEntryName));
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void listFiles(Collection<SimpleFileSystem> ret, File directory) {
        File[] contents = new File(directory, path).listFiles();
        if (contents != null) {
            int baseLength = directory.getPath().length() + 1;
            for (File content : contents) {
                String contentPath = content.getPath();
                String newPath = contentPath.substring(baseLength);
                if (File.separatorChar == '\\') {
                    newPath = newPath.replace('\\', '/');
                }
                ret.add(new LocalFileSystem(directory, newPath));
            }
        }
    }

    private static File toFile(URL url) {
        try {
            return Paths.get(url.toURI()).toFile();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareTo(SimpleFileSystem that) {
        return this.path.compareTo(that.path);
    }

    public String getName() {
        int i = path.lastIndexOf('/');
        return path.substring(i + 1);
    }

    public InputStream createInputStream() throws IOException {
        for (ClassLoader classLoader = getClass().getClassLoader();
             classLoader != null;
             classLoader = classLoader.getParent()) {
            if (classLoader instanceof URLClassLoader) {
                URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
                URL[] urls = urlClassLoader.getURLs();
                if (urls != null) {
                    for (URL url : urls) {
                        if ("file".equals(url.getProtocol())) {
                            File file = toFile(url);
                            if (file.isFile()) {
                                String fileName = file.getName().toLowerCase();
                                if (fileName.endsWith(".jar") || fileName.endsWith(".zip")) {
                                    InputStream zipInputStream = createZipInputStream(file);
                                    if (zipInputStream != null) {
                                        return zipInputStream;
                                    }
                                }
                            } else {
                                File content = new File(file, path);
                                if (content.isFile()) {
                                    return Files.newInputStream(content.toPath());
                                }
                            }
                        }
                    }
                }
            }
        }
        throw new RuntimeException("Not found: " + this);
    }

    InputStream createZipInputStream(File file) throws IOException {
        try (ZipInputStream zipInputStream = new ZipInputStream(new BufferedInputStream(Files.newInputStream(file.toPath())))) {
            while (true) {
                ZipEntry zipEntry = zipInputStream.getNextEntry();
                if (zipEntry == null) break;
                String zipEntryName = trimSlashes(zipEntry.getName());
                if (zipEntryName.equals(path)) {
                    return zipInputStream;
                }
            }
        }
        return null;
    }
}
