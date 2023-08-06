package org.dandoy.dbpopd.setup;

import io.micronaut.context.annotation.Property;
import io.micronaut.context.event.ApplicationEventListener;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.dandoy.dbpop.datasets.Datasets;
import org.dandoy.dbpopd.config.ConnectionType;
import org.dandoy.dbpopd.config.DatabaseCacheChangedEvent;
import org.dandoy.dbpopd.datasets.DatasetsService;
import org.dandoy.dbpopd.populate.PopulateService;

import java.util.List;

@Singleton
@Slf4j
public class DatasetLoaderService implements ApplicationEventListener<DatabaseCacheChangedEvent> {
    private final PopulateService populateService;
    private final DatasetsService datasetsService;
    // When running the tests, we don't want the data to be preloaded
    private final boolean loadDatasets;
    private boolean hasLoadedDatasets;

    @SuppressWarnings("MnInjectionPoints")
    public DatasetLoaderService(
            PopulateService populateService,
            DatasetsService datasetsService,
            @Property(name = "dbpopd.startup.loadDatasets", defaultValue = "true") boolean loadDatasets
    ) {
        this.populateService = populateService;
        this.datasetsService = datasetsService;
        this.loadDatasets = loadDatasets;
    }

    @Override
    public void onApplicationEvent(DatabaseCacheChangedEvent event) {
        if (event.type() == ConnectionType.TARGET) {
            if (loadDatasets && !hasLoadedDatasets) {
                try {
                    if (datasetsService.canPopulate(Datasets.BASE)) {
                        populateService.populate(List.of(Datasets.STATIC, Datasets.BASE));
                    }
                    hasLoadedDatasets = true;
                } catch (Exception e) {
                    // Do not fail the startup because of a dataset error
                    log.error("Failed to load the static and base datasets", e);
                }
            }
        }
    }
}
