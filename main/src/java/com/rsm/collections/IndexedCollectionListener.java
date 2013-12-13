package com.rsm.collections;

/**
 * Created by Raymond on 12/4/13.
 */
public interface IndexedCollectionListener<T extends IndexedCollection> {

    void onResize(T collection, int oldSize, int newSize);
}
