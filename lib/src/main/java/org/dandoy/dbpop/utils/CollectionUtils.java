package org.dandoy.dbpop.utils;

import java.util.ArrayList;
import java.util.Collection;
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

    public static <T> List<T> concat(Collection<T> t1, Collection<T> t2) {
        ArrayList<T> ret = new ArrayList<>(t1);
        ret.addAll(t2);
        return ret;
    }
}
