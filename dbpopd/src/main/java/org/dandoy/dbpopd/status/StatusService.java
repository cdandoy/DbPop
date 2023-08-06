package org.dandoy.dbpopd.status;

import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Singleton
@Getter
@Setter
@Slf4j
public class StatusService {
    @Getter
    static class Status {
        private final String name;
        private boolean running;
        private String error;

        public Status(String name) {
            this.name = name;
            this.running = true;
        }
    }

    public interface StatusRunnable {
        void run() throws Exception;
    }

    public interface StatusSupplier<T> {
        T get() throws Exception;
    }

    private List<Status> statuses = new ArrayList<>();
    private boolean complete;

    public void run(String name, StatusRunnable runnable) {
        run(name, () -> {
            runnable.run();
            return null;
        });
    }

    public <T> T run(String name, StatusSupplier<T> runnable) {
        Status status = new Status(name);
        statuses.add(status);
        try {
            return runnable.get();
        } catch (Exception e) {
            log.error(name, e);
            status.error = e.getMessage();
            throw new RuntimeException(e);
        } finally {
            status.running = false;
        }
    }
}
