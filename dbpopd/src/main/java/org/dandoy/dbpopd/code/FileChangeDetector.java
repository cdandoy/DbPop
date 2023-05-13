package org.dandoy.dbpopd.code;

import io.micronaut.context.annotation.Context;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.database.ObjectIdentifier;
import org.dandoy.dbpopd.ConfigurationService;
import org.dandoy.dbpopd.utils.DbPopdFileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.*;

@Singleton
@Slf4j
@Context
public class FileChangeDetector {
    private final File codeDirectory;
    private WatchService watchService;
    private Thread thread;
    private final Path codePath;
    private final ChangeDetector changeDetector;

    public FileChangeDetector(ConfigurationService configurationService, ChangeDetector changeDetector) {
        codeDirectory = configurationService.getCodeDirectory();
        codePath = configurationService.getCodeDirectory().toPath();
        this.changeDetector = changeDetector;
    }

    @PostConstruct
    void postContruct() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
            thread = new Thread(this::threadLoop, "FileChangeDetector");
            thread.setDaemon(true);
            thread.start();
            try (Stream<Path> stream = Files.walk(codePath)) {
                stream
                        .filter(Files::isDirectory)
                        .forEach(this::watch);
            }
            for (Path path = codePath.getParent(); path != null; path = path.getParent()) {
                if (Files.isDirectory(path)) {
                    watch(path);
                }
            }
        } catch (IOException e) {
            log.error("Cannot watch the directory " + codePath, e);
        }

    }

    @PreDestroy
    void preDestroy() {
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
            path.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        } catch (IOException e) {
            log.error("Failed to register the path " + path, e);
        }
    }

    void threadLoop() {
        while (true) {
            try {
                WatchKey watchKey = watchService.take();
                if (watchKey == null) break;
                List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
                for (WatchEvent<?> pollEvent : watchEvents) {
                    // Check that the event is about Paths
                    if (pollEvent.context() instanceof Path childPath && watchKey.watchable() instanceof Path parentPath) {
                        Path path = parentPath.resolve(childPath);
                        if (path.startsWith(codePath)) {
                            // The event is withing .../code/
                            if (pollEvent.kind() == ENTRY_CREATE) {
                                onEntryCreated(path);
                            } else if (pollEvent.kind() == ENTRY_DELETE) {
                                onEntryDeleted(watchKey, path);
                            } else if (pollEvent.kind() == ENTRY_MODIFY) {
                                onEntryModified(path);
                            }
                        } else if (codePath.startsWith(path)) {
                            if (Files.isDirectory(path)) {
                                // The event is about a directory above .../code/
                                if (pollEvent.kind() == ENTRY_CREATE) {
                                    watch(path);
                                } else if (pollEvent.kind() == ENTRY_DELETE) {
                                    watchKey.cancel();
                                }
                            }
                        }
                    }
                }
                watchKey.reset();
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

    private void onEntryCreated(Path path) {
        log.debug("FileChangeDetector.onEntryCreated: {}", path);
        if (Files.isDirectory(path)) {
            watch(path);
        }
        scan(path);
    }

    private void onEntryModified(Path path) {
        log.debug("FileChangeDetector.onEntryModified: {}", path);
        if (!Files.isDirectory(path)) {
            scan(path);
        }
    }

    private void onEntryDeleted(WatchKey watchKey, Path path) {
        log.debug("FileChangeDetector.onEntryDeleted: {}", path);
        if (Files.isDirectory(path)) {
            watchKey.cancel();
        } else {
            File file = path.toFile();
            ObjectIdentifier objectIdentifier = DbPopdFileUtils.toObjectIdentifier(codeDirectory, file);
            if (objectIdentifier != null) {
                changeDetector.whenFileChanged(null, objectIdentifier);
            }
        }
    }

    private void scan(Path path) {
        try {
            if (Files.isDirectory(path)) {
                try (Stream<Path> stream = Files.list(path)) {
                    stream.forEach(this::scan);
                }
            } else {
                File file = path.toFile();
                ObjectIdentifier objectIdentifier = DbPopdFileUtils.toObjectIdentifier(codeDirectory, file);
                if (objectIdentifier != null) {
                    changeDetector.whenFileChanged(file, objectIdentifier);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
