package org.dandoy.dbpop.utils;

import java.util.ArrayList;
import java.util.Collection;

/**
 * This utility class compares two collections and split the element in 3 categories:
 * <ul>
 *   <li>leftOnly: only exist in the first collection</li>
 *   <li>common: are common to both</li>
 *   <li>rightOnly: only exist in the second collection.</li>
 * </ul>
 */
public class CollectionComparator<T> {
    public final ArrayList<T> leftOnly;
    public final ArrayList<T> common;
    public final ArrayList<T> rightOnly;

    private CollectionComparator(ArrayList<T> leftOnly, ArrayList<T> common, ArrayList<T> rightOnly) {
        this.leftOnly = leftOnly;
        this.common = common;
        this.rightOnly = rightOnly;
    }

    public static <T> CollectionComparator<T> build(Collection<T> left, Collection<T> right) {
        ArrayList<T> leftOnly = new ArrayList<>(left);
        ArrayList<T> common = new ArrayList<>(left);
        ArrayList<T> rightOnly = new ArrayList<>(right);
        leftOnly.removeAll(right);
        common.retainAll(right);
        rightOnly.removeAll(left);
        return new CollectionComparator<>(leftOnly, common, rightOnly);
    }
}
