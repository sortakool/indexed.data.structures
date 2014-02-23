package com.rsm.collections;

/**
 * Created by Raymond on 12/4/13.
 */
public interface EntryIterator {

    boolean hasNext();
    int nextEntry();
    void reset(int[] entries);
}
