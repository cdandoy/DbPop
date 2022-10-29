package org.dandoy.dbpop.fs;

import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.utils.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class ClasspathCollector {
    private static final Pattern URL_PATTERN = Pattern.compile("^([^:]*):.*$");
    private final List<File> ret = new ArrayList<>();
    private final Set<File> found = new HashSet<>();

    @SuppressWarnings("unused")
    List<File> collectClasspathFiles() {
        return collectClasspathFiles(ClasspathCollector.class);
    }

    List<File> collectClasspathFiles(Class<?> cls) {
        return collectClasspathFiles(cls.getClassLoader());
    }

    List<File> collectClasspathFiles(ClassLoader classLoader) {
        if (URLClassLoader.class.isAssignableFrom(classLoader.getClass())) {
            collectClasspathFiles_java8(classLoader);
        } else {
            collectClasspathFiles_postJava8();
        }
        return ret;
    }

    private void collectClasspathFiles_postJava8() {
        String classPath = System.getProperty("java.class.path");
        if (classPath != null) {
            List<String> classpath = StringUtils.split(classPath, File.pathSeparatorChar);
            for (String path : classpath) {
                File file = new File(path);
                addFile(file);
            }
        }
    }

    private void collectClasspathFiles_java8(ClassLoader classLoader) {
        for (; classLoader != null; classLoader = classLoader.getParent()) {
            if (classLoader instanceof URLClassLoader) {
                URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
                URL[] urls = urlClassLoader.getURLs();
                if (urls != null) {
                    for (URL url : urls) {
                        addUrl(url);
                    }
                }
            }
        }
    }

    void addManifestJars(File file) {
        try (JarFile jarFile = new JarFile(file)) {
            Manifest manifest = jarFile.getManifest();
            if (manifest != null) {
                Attributes mainAttributes = manifest.getMainAttributes();
                String classPath = mainAttributes.getValue(Attributes.Name.CLASS_PATH);
                if (classPath != null) {
                    List<String> paths = StringUtils.split(classPath, ' ');
                    addClasspaths(file, paths);
                }
            }
        } catch (IOException e) {
            log.debug("Cannot read: " + file, e);
        }
    }

    private void addClasspaths(File file, List<String> paths) {
        for (String path : paths) {
            if (!path.isEmpty()) {
                Matcher matcher = URL_PATTERN.matcher(path);
                if (matcher.matches()) {
                    try {
                        URL url = new URL(path);
                        addUrl(url);
                    } catch (MalformedURLException e) {
                        log.debug("Invalid URL: " + path, e);
                    }
                } else {
                    File referencedFile = isRelative(path) ? new File(file.getParentFile(), path) : new File(path);
                    addFile(referencedFile);
                }
            }
        }
    }

    @SuppressWarnings("RedundantIfStatement")
    private static boolean isRelative(String path) {
        if (path.startsWith("/")) return false;
        if (File.pathSeparatorChar == '\\') {
            if (path.startsWith("\\")) return false;
            if (path.length() > 2) {
                char drive = Character.toLowerCase(path.charAt(0));
                if ('a' <= drive && drive <= 'z') {
                    if (path.charAt(1) == ':') {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private void addUrl(URL url) {
        if ("file".equals(url.getProtocol())) {
            addFile(toFile(url));
        }
    }

    private void addFile(File file) {
        if (file.exists()) {
            if (found.add(file)) {
                ret.add(file);
                if (file.getName().toLowerCase().endsWith(".jar")) {
                    addManifestJars(file);
                }
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
}
