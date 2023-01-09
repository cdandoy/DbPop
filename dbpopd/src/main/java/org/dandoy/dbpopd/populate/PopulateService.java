package org.dandoy.dbpopd.populate;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Singleton
@Slf4j
public class PopulateService {
    private final PopulatorHolder populatorHolder;

    public PopulateService(PopulatorHolder populatorHolder) {
        this.populatorHolder = populatorHolder;
    }

    public PopulateResult populate(List<String> dataset) {
        long t0 = System.currentTimeMillis();
        int rows = populatorHolder.getPopulator().load(dataset);
        long t1 = System.currentTimeMillis();
        return new PopulateResult(rows, t1 - t0);
    }

    /**
     * Used for tests only
     */
    public void resetPopulatorHolder() {
        populatorHolder.reset();
    }

    public record PopulateResult(int rows, long millis) {}
}
