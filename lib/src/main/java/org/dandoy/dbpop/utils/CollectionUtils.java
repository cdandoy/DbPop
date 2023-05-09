package org.dandoy.dbpop.utils;

import java.util.List;
import java.util.function.Consumer;

public class CollectionUtils {
    public static <T> void partition(List<T> list, int size, Consumer<List<T>> consumer) {
        int pos = 0;
        while (pos < list.size()) {
            int max = Math.min(list.size() - pos, size);
            consumer.accept(list.subList(pos, pos + max));
            pos += max;
        }
    }
}
