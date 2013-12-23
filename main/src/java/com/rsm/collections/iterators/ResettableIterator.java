package com.rsm.collections.iterators;

import java.util.Iterator;

/**
 * Created by Raymond on 12/20/13.
 */
public interface ResettableIterator<T> extends Iterator<T> {

    /**
     * Resets the iterator back to the position at which the iterator
     * was created.
     */
    void reset();
}
