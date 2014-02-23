package com.rsm.collections;

/**
 * Created by Raymond on 12/5/13.
 */
public class EntryIteratorImpl implements EntryIterator {

    public static final int LOOP_COMPLETED = -1;

    private int[] myEntries;

    private int lastEntry = LOOP_COMPLETED;

    /**
     * Index of element to be returned by subsequent call to next.
     */
    private int next = LOOP_COMPLETED;

    @Override
    public void reset(int[] entries) {
        this.myEntries = entries;
        lastEntry = LOOP_COMPLETED;
        next = LOOP_COMPLETED;
        for(int i=0; i<entries.length; i++) {
            if(entries[i] != IndexedCollection.DEFAULT_INDEX) {
                next = i;
                break;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return (next != LOOP_COMPLETED);
    }

    @Override
    public int nextEntry() {
        lastEntry = next;
        int i=lastEntry+1;
        next = LOOP_COMPLETED;
        for( ; i<myEntries.length; i++) {
            if(myEntries[i] != IndexedCollection.DEFAULT_INDEX) {
                next = i;
                break;
            }
        }
        return lastEntry;
    }
}
