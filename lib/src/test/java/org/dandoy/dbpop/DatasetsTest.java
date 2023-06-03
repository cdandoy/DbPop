package org.dandoy.dbpop;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.dandoy.dbpop.datasets.Datasets.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DatasetsTest {
    @Test
    void name() {
        for (int i = 0; i < 1024; i++) {
            List<String> start = List.of(STATIC, BASE, "aaa", "bbb", "ccc");
            ArrayList<String> list = new ArrayList<>(start);
            Collections.shuffle(list);
            list.sort(DATASET_NAME_COMPARATOR);
            assertEquals(start, list);
        }
    }
}