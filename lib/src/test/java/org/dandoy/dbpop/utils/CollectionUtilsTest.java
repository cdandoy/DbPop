package org.dandoy.dbpop.utils;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CollectionUtilsTest {
    @Test
    void name() {
        {
            AtomicInteger count = new AtomicInteger();
            CollectionUtils.partition(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2, objects -> {
                assertEquals(2, objects.size());
                count.getAndIncrement();
            });
            assertEquals(5, count.get());
        }
        {
            AtomicInteger count = new AtomicInteger();
            CollectionUtils.partition(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 5, objects -> {
                assertEquals(5, objects.size());
                count.getAndIncrement();
            });
            assertEquals(2, count.get());
        }
        {
            AtomicInteger count = new AtomicInteger();
            CollectionUtils.partition(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), 5, objects -> {
                assertTrue(objects.size() == 5 || objects.size() == 1);
                count.getAndIncrement();
            });
            assertEquals(3, count.get());
        }
    }
}