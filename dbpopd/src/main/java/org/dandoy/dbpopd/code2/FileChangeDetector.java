package org.dandoy.dbpopd.code2;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.*;

@Slf4j
public class FileChangeDetector implements AutoCloseable {
    private WatchService watchService;
    private Thread thread;
    private final Path codePath;
    private final FileChangeListener fileChangeListener;
    private final Map<WatchKey, Path> keys = new HashMap<>();
    private final Set<Path> knownFiles = new HashSet<>();
    /**
     * Accumulates the actions and only send the events if nothing happened for a few seconds.
     * The boolean is true if the file has been deleted
     */
    private final Map<File, Boolean> pendingChangedFiles = new HashMap<>();

    private FileChangeDetector(Path codePath, FileChangeListener fileChangeListener) {
        this.codePath = codePath;
        this.fileChangeListener = fileChangeListener;
    }

    public static FileChangeDetector createFileChangeDetector(Path codePath, FileChangeListener fileChangeListener) {
        try {
            FileChangeDetector detector = new FileChangeDetector(codePath, fileChangeListener);
            detector.checkAllFiles();
            detector.watchService = FileSystems.getDefault().newWatchService();
            detector.thread = new Thread(detector::threadLoop, "FileChangeDetector");
            detector.thread.setDaemon(true);
            detector.thread.start();
            if (Files.isDirectory(codePath)) {
                detector.watchPathAndSubPaths(codePath);
            }
            for (Path path = codePath.getParent(); path != null; path = path.getParent()) {
                detector.watch(path);
            }
            return detector;
        } catch (IOException e) {
            log.error("Cannot watch the directory " + codePath, e);
            return null;
        }
    }

    @Override
    public void close() {
        try {
            if (watchService != null) {
                watchService.close();
            }
        } catch (IOException e) {
            log.error("Failed to stop the WatchService");
            return;
        }
        try {
            thread.join(2000);
            if (thread.isAlive()) {
                log.info("Failed to stop the thread");
            }
        } catch (InterruptedException e) {
            log.info("Failed to stop the thread", e);
        }
    }

    private void watch(Path path) {
        try {
            log.debug("Watching {}", path);
            WatchKey watchKey = path.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            keys.put(watchKey, path);
        } catch (IOException e) {
            log.error("Failed to register the path " + path, e);
        }
    }

    private void watchPathAndSubPaths(Path path) {
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes attrs) {
                    watch(path);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                    addKnownFile(path);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.error("registerAll failed", e);
        }
    }

    private void handlePathChanged(Path path) {
        File file = path.toFile();
        log.debug("File modified: {}", file);
        pendingChangedFiles.put(file, false);
    }

    private void handlePathDeleted(Path path) {
        File file = path.toFile();
        log.debug("File deleted: {}", file);
        pendingChangedFiles.put(file, true);
    }

    void threadLoop() {
        while (true) {
            try {
                WatchKey watchKey = getWatchKey();
                if (watchKey != null) {
                    List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
                    for (WatchEvent<?> pollEvent : watchEvents) {
                        if (pollEvent.kind() == OVERFLOW) continue;

                        // Check that the event is about Paths
                        if (pollEvent.context() instanceof Path childPath) {
                            log.debug("WatchEvent {}: {}", pollEvent.kind(), childPath);
                            Path parentPath = keys.get(watchKey);
                            Path path = parentPath.resolve(childPath);

                            if (pollEvent.kind() == ENTRY_CREATE) {
                                if (Files.isDirectory(path)) {
                                    watchPathAndSubPaths(path);
                                } else {
                                    addKnownFile(path);
                                    handlePathChanged(path);
                                }
                            } else if (pollEvent.kind() == ENTRY_DELETE) {
                                if (removeKnownFile(path)) {
                                    handlePathDeleted(path);
                                }
                            } else if (pollEvent.kind() == ENTRY_MODIFY) {
                                if (knownFiles.contains(path)) {
                                    handlePathChanged(path);
                                } else {
                                    log.debug("Unknown file modified: {}", path);
                                }
                            }
                        }
                    }
                    boolean valid = watchKey.reset();
                    if (!valid) {
                        Path removed = keys.remove(watchKey);
                        log.debug("No longer watching {}", removed);
                        if (keys.isEmpty()) {
                            log.error("FileChangeDetector has nothing left to watch");
                        }
                    }
                }
            } catch (ClosedWatchServiceException ignored) {
                log.debug("FileChangeDetector - WatchService closed");
                break;
            } catch (InterruptedException e) {
                log.debug("FileChangeDetector interrupted");
                break;
            }
        }
        log.debug("FileChangeDetector thread stopped");
    }

    private void addKnownFile(Path path) {
        log.debug("addKnownFile: {}", path);
        knownFiles.add(path);
    }

    private boolean removeKnownFile(Path path) {
        log.debug("removeKnownFile: {}", path);
        return knownFiles.remove(path);
    }

    private WatchKey getWatchKey() throws InterruptedException {
        WatchKey watchKey;
        if (pendingChangedFiles.isEmpty()) {
            watchKey = watchService.take();
        } else {
            watchKey = watchService.poll(1, TimeUnit.SECONDS);
            if (watchKey == null) {
                log.debug("Processing {} events", pendingChangedFiles.size());
                fileChangeListener.whenFilesChanged(this.pendingChangedFiles);
                this.pendingChangedFiles.clear();
                return null;
            }
        }
        return watchKey;
    }

    private void checkAllFiles() {
        try {
            Map<File, Boolean> changes = new HashMap<>();
            Files.walkFileTree(codePath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                    changes.put(path.toFile(), false);
                    return FileVisitResult.CONTINUE;
                }
            });
            this.fileChangeListener.whenFilesChanged(changes);
        } catch (IOException e) {
            log.error("Failed to checkAllFiles()", e);
        }
    }

    public interface FileChangeListener {
        /**
         * @param changes The key is the file that has changed, the value is true if the file has been deleted.
         */
        void whenFilesChanged(Map<File, Boolean> changes);
    }
}
