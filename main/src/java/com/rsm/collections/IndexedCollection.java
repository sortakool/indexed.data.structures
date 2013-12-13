package com.rsm.collections;

/**
 * Created by Raymond on 12/4/13.
 */
public interface IndexedCollection {

    int DEFAULT_INDEX = -1;
    int DEFAULT_KEY_INDEX = -2;

    void addCollectionListeners(IndexedCollectionListener... listeners);
    void removeCollectionListeners(IndexedCollectionListener... listeners);

    void print(StringBuilder sb);
}
