package org.dandoy.dbpop.utils;

@SuppressWarnings("unused")
public class ElapsedStopWatch {
    private long t0;

    public ElapsedStopWatch() {
        t0 = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        long t = System.currentTimeMillis() - t0;
        t0 = System.currentTimeMillis();
        return t + "ms";
    }
}
