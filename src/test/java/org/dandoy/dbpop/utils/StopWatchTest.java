package org.dandoy.dbpop.utils;

import org.junit.jupiter.api.Test;

class StopWatchTest {
    @Test
    void name() {
        StopWatch.enable();

        try (StopWatch ignored = StopWatch.record("test one")) {
            pause(100);
        }

        StopWatch.record("test two", () -> pause(200));

        StopWatch.record("test three", () -> {
            pause(200);
            return "result3";
        });

        StopWatch.report(System.out);
    }

    static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}