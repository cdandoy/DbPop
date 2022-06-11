package org.dandoy;

public class ElapsedStopWatch {
    private long t = System.currentTimeMillis();

    public String toString() {
        long t0 = t;
        t = System.currentTimeMillis();
        return String.format("%dms", t - t0);
    }
}
