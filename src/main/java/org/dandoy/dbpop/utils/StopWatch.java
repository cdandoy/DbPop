package org.dandoy.dbpop.utils;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class StopWatch implements AutoCloseable {
    private static final Map<String, Record> records = new HashMap<>();
    private static final StopWatch NOOP = new StopWatch(null, 0);
    private static boolean enabled;
    private final String watch;
    private final long t0;

    private StopWatch(String watch, long t0) {
        this.watch = watch;
        this.t0 = t0;
    }

    public static void report(PrintStream out) {
        List<String[]> strings = records.entrySet().stream()
                .sorted(Comparator.comparingLong(value -> -value.getValue().timeInMillis))
                .map(entry -> new String[]{
                        entry.getKey(),
                        String.valueOf(entry.getValue().timeInMillis / 1_000_000),
                        String.valueOf(entry.getValue().nbrCalls),
                        String.valueOf(entry.getValue().timeInMillis / 1_000_000 / entry.getValue().nbrCalls)
                })
                .collect(Collectors.toList());
        int len0 = strings.stream().mapToInt(value -> value[0].length()).max().orElse(0);
        int len1 = strings.stream().mapToInt(value -> value[1].length()).max().orElse(0);
        int len2 = strings.stream().mapToInt(value -> value[2].length()).max().orElse(0);
        int len3 = strings.stream().mapToInt(value -> value[3].length()).max().orElse(0);
        strings.forEach(it -> {
            String fmt0 = "%-" + len0 + "s";
            String fmt1 = "%" + len1 + "sms";
            String fmt2 = "%" + len2 + "s calls";
            String fmt3 = "%" + len3 + "sms/call";
            out.printf(fmt0 + " " + fmt1 + " " + fmt2 + " " + fmt3 + "%n", it[0], it[1], it[2], it[3]);
        });
    }

    public static void enable() {
        enabled = true;
    }

    @SuppressWarnings("unused")
    public static void disable() {
        enabled = false;
    }

    @Override
    public void close() {
        if (t0 == 0) return;
        record(watch, t0);
    }

    public static StopWatch record(String watch) {
        if (!enabled) return NOOP;
        long t = System.nanoTime();
        return new StopWatch(watch, t);
    }

    public static void record(String watch, Runnable runnable) {
        if (enabled) {
            long t0 = System.nanoTime();
            try {
                runnable.run();
            } finally {
                record(watch, t0);
            }
        } else {
            runnable.run();
        }
    }

    public static <T> T record(String watch, Supplier<T> supplier) {
        if (enabled) {
            long t0 = System.nanoTime();
            try {
                return supplier.get();
            } finally {
                record(watch, t0);
            }
        } else {
            return supplier.get();
        }
    }

    private static synchronized void record(String watch, long t0) {
        long t1 = System.nanoTime();
        records.computeIfAbsent(watch, s -> new Record())
                .set(t0, t1);
    }

    public static class Record {
        public long timeInMillis;
        public int nbrCalls;

        void set(long t0, long t1) {
            timeInMillis += t1 - t0;
            nbrCalls++;
        }
    }
}
