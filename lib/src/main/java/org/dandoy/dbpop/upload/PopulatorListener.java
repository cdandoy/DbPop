package org.dandoy.dbpop.upload;

public interface PopulatorListener {
    void afterPopulate();

    void afterPopulate(String dataset);
}
