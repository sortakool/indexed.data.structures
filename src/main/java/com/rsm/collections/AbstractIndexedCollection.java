package com.rsm.collections;

import com.rsm.collections.IndexedCollection;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Raymond on 12/4/13.
 */
public abstract class AbstractIndexedCollection implements IndexedCollection {

    protected Set<IndexedCollectionListener> indexedCollectionListeners = new HashSet<>(2);

    @Override
    public void addCollectionListeners(IndexedCollectionListener... listeners) {
        if(indexedCollectionListeners != null) {
            for (IndexedCollectionListener listener : indexedCollectionListeners) {
                indexedCollectionListeners.add(listener);
            }
        }
    }

    @Override
    public void removeCollectionListeners(IndexedCollectionListener... listeners) {
        if(indexedCollectionListeners != null) {
            for (IndexedCollectionListener listener : indexedCollectionListeners) {
                indexedCollectionListeners.remove(listener);
            }
        }
    }

    protected void notifyResize(int oldSize, int newSize) {
        for (IndexedCollectionListener listener : indexedCollectionListeners) {
            listener.onResize(this, oldSize, newSize);
        }
    }

}
