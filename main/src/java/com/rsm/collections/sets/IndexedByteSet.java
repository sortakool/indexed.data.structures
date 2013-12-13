package com.rsm.collections.sets;

import com.rsm.collections.EntryIterator;
import com.rsm.collections.IndexedCollection;

/**
 * Created by Raymond on 12/4/13.
 */
public interface IndexedByteSet extends IndexedCollection {
    short MAX_VALUES = (short)(Math.abs(Byte.MIN_VALUE) + Byte.MAX_VALUE);
    byte DEFAULT_KEY = Byte.MIN_VALUE;

    void clear();

    int getCapacity();

    float getLoadFactor();

    int getMaxFill();

    int getSize();

    int add(byte k);

    int getIndex(byte k);

    byte getKey(int index);

    byte removeIndex(int index);

    int remove(byte k);

    int getLastIndex();

    void initializeEntryIterator(EntryIterator destination);

    EntryIterator generateEntryIterator();

    interface ByteKeysIterator {
        boolean hasNext();
        byte next();
        void reset();
    }


    void initializeKeysIterator(ByteKeysIterator keysIterator);
    ByteKeysIterator generateKeysIterator();
}
