package com.rsm.collections.sets;

import com.rsm.collections.AbstractIndexedCollection;
import com.rsm.collections.EntryIterator;
import com.rsm.collections.EntryIteratorImpl;
import it.unimi.dsi.fastutil.HashCommon;

import java.util.Arrays;

/**
 * Created by Raymond on 12/4/13.
 */
public class IndexedByteOpenHashSet3 extends AbstractIndexedCollection implements IndexedByteSet {

    /**
     * The array of keys
     */
    protected transient byte[] keys;

    /**
     * The array telling whether a position is used.
     */
    protected transient boolean used[];

    /**
     * The acceptable load factor.
     */
    protected final float loadFactor;

    /**
     * the current collection capcity
     */
    protected transient int capacity;

    /**
     * Threshold after which we rehash. It must be the collection size times loadFactor.
     */
    protected transient int maxFill;

    /**
     * The number of entries in the set.
     */
    protected int size;

    /**
     * The mask for wrapping a position counter.
     */
    protected transient int mask;

    /**
     * Contains index position to key array.
     */
    protected int[] indexes2KeysIndexes;

    /**
     * Reverse mapping of keys to indexes.
     */
    protected int[] keysIndexes2Indexes;

    protected int nextIndex, minIndex, maxIndex = DEFAULT_INDEX;

    /**
     * Returns the last index saved in a call to getIndex.
     */
    protected int lastIndex;

    @Override
    public void print(StringBuilder sb) {
        sb.append("IndexedByteOpenHashSet:")
                .append("[loadFactor=").append(loadFactor).append("]")
                .append("[maxFill=").append(maxFill).append("]")
                .append("[mask=").append(mask).append("]")
                .append("[size=").append(size).append("]")
                .append("[capacity=").append(capacity).append("]")
                .append("[nextIndex=").append(nextIndex).append("]")
                .append("[minIndex=").append(minIndex).append("]")
                .append("[maxIndex=").append(maxIndex).append("]")
                .append("[lastIndex=").append(lastIndex).append("]")
                .append("\n")
        ;
        for (int i = 0; i < keys.length; i++) {
            sb
                    .append("[i=").append(i).append("]")
                    .append("[used=").append(used[i]).append("]")
                    .append("[key=").append(keys[i]).append("]")
                    .append("[indexes2KeysIndexes=").append(indexes2KeysIndexes[i]).append("]")
                    .append("[keysIndexes2Indexes=").append(keysIndexes2Indexes[i]).append("]")
                    .append("\n")

            ;
        }
    }

    public IndexedByteOpenHashSet3(final int expected, final float loadFactor) {
        if( (loadFactor <= 0) || (loadFactor > 1) ) {
            throw new IllegalArgumentException("Load factor must be between 0 and 1");
        }
        if(expected < 0) {
            throw new IllegalArgumentException("Expected must be positive");
        }
        if(expected > MAX_VALUES) {
            throw new IllegalArgumentException("expected must be less than "  +MAX_VALUES);
        }
        this.loadFactor = loadFactor;
        this.capacity = HashCommon.arraySize(expected, loadFactor);
        this.mask = capacity - 1;
        this.maxFill = HashCommon.maxFill(this.capacity, loadFactor);
        this.keys = new byte[this.capacity];
        this.used = new boolean[this.capacity];

        size = 0;
        Arrays.fill(keys, DEFAULT_KEY);

        indexes2KeysIndexes = new int[capacity];
        Arrays.fill(indexes2KeysIndexes, DEFAULT_INDEX);
        keysIndexes2Indexes = new int[this.capacity];
        Arrays.fill(keysIndexes2Indexes, DEFAULT_KEY);
        nextIndex = DEFAULT_INDEX;
        minIndex = DEFAULT_INDEX;
        maxIndex = DEFAULT_INDEX;
        lastIndex = DEFAULT_INDEX;
    }

    @Override
    public void clear() {
        size = 0;
        Arrays.fill(used, false);
        Arrays.fill(keys, DEFAULT_KEY);
        Arrays.fill(indexes2KeysIndexes, DEFAULT_INDEX);
        Arrays.fill(keysIndexes2Indexes, DEFAULT_KEY);
        nextIndex = DEFAULT_INDEX;
        minIndex = DEFAULT_INDEX;
        maxIndex = DEFAULT_INDEX;
        lastIndex = DEFAULT_INDEX;

    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public float getLoadFactor() {
        return loadFactor;
    }

    @Override
    public int getMaxFill() {
        return maxFill;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public int add(byte key) {
        //the starting point.
        int keysIndex = HashCommon.murmurHash3(key) & mask;
        //there's always an unused entry.
        while(used[keysIndex]) {
            if(key == keys[keysIndex]) {
                int index = keysIndexes2Indexes[keysIndex];
                assert (index != DEFAULT_INDEX);
                assert (indexes2KeysIndexes[index] == keysIndex);
                return index;
            }
            keysIndex = (keysIndex + 1) & mask;
        }
        nextIndex = getNextFreeIndex();
        minIndex = Math.max(0, Math.min(nextIndex, minIndex));
        maxIndex = Math.max(nextIndex, maxIndex);

        assert (nextIndex != DEFAULT_INDEX);
        assert (minIndex <= nextIndex);
        assert (nextIndex <= maxIndex);

        indexes2KeysIndexes[nextIndex] = keysIndex;
        keysIndexes2Indexes[keysIndex] = nextIndex;
        used[keysIndex] = true;
        keys[keysIndex] = key;
        if(++size >= maxFill) {
            int oldCapacity = capacity;
            rehash(HashCommon.arraySize(size+1, loadFactor));
            notifyResize(oldCapacity, capacity);
        }

        return nextIndex;
    }

    /**
     * Resizes the set.
     * <p/>
     * <p>
     * This method implements the basic rehashing strategy, and may be
     * overridden by subclasses implmennting different rehashing strategies
     * (e.g., disk-based rehashing). However, you should not override this method unless
     * you understand the internal workings of this class.
     * </p>
     * @param newN
     */
    protected void rehash(final int newN) {
        final int newMask = newN - 1;
        final boolean newUsed[] = new boolean[newN];
        byte[] newKeys = new byte[newN];
        int[] newIndexes2KeysIndexes = new int[newN];
        int[] newKeysIndexes2Indexes = new int[newN];

        Arrays.fill(newKeys, DEFAULT_KEY);
        Arrays.fill(newIndexes2KeysIndexes, DEFAULT_INDEX);
        Arrays.fill(newKeysIndexes2Indexes, DEFAULT_KEY);

        for(int i=0; i< indexes2KeysIndexes.length; i++) {
            int keysIndex = indexes2KeysIndexes[i];
            if(keysIndex != DEFAULT_INDEX) {
                byte key = keys[keysIndex];
                keysIndex = HashCommon.murmurHash3(key) & newMask;
                while(newUsed[keysIndex]) {
                    keysIndex = (keysIndex + 1) & newMask;
                }
                newUsed[keysIndex] = true;
                newKeys[keysIndex] = key;
                newIndexes2KeysIndexes[i] = keysIndex;
                newKeysIndexes2Indexes[keysIndex] = i;
            }
        }
        capacity = newN;
        mask = newMask;
        maxFill = HashCommon.maxFill(capacity, loadFactor);
        this.keys = newKeys;
        this.used = newUsed;
        this.indexes2KeysIndexes = newIndexes2KeysIndexes;
        this.keysIndexes2Indexes = newKeysIndexes2Indexes;
    }

    private int getNextFreeIndex() {
        for(int i=nextIndex+1; i< indexes2KeysIndexes.length; i++) {
            if(indexes2KeysIndexes[i] == DEFAULT_INDEX) {
                return i;
            }
        }
        for(int i=minIndex; i<nextIndex; i++) {
            if(indexes2KeysIndexes[i] == DEFAULT_INDEX) {
                return i;
            }
        }
        throw new RuntimeException("Unable to find next index. Where [minIndex="+minIndex+"][nextIndex="+nextIndex+"][maxIndex="+maxIndex+"]");
    }

    @Override
    public int getIndex(byte key) {
        // the starting point
        int keysIndex = HashCommon.murmurHash3(key) & mask;
        // there's always an unused entry.
        while(used[keysIndex]) {
            if(key == keys[keysIndex]) {
                int index = keysIndexes2Indexes[keysIndex];
                assert (index != DEFAULT_INDEX);
                assert (indexes2KeysIndexes[index] == keysIndex);
                lastIndex = index;  //set this just in case a subsequent add call is made
                return index;
            }
            keysIndex = (keysIndex + 1) & mask;
        }
        lastIndex = DEFAULT_INDEX;
        return DEFAULT_INDEX;
    }

    @Override
    public byte getKey(int index) {
        int keysIndex = indexes2KeysIndexes[index];
        if(keysIndex != DEFAULT_KEY) {
            byte key = keys[keysIndex];
            assert ( used[keysIndex] );
            assert ( keysIndexes2Indexes[keysIndex] ==  index);
            return key;
        }
        return DEFAULT_KEY;
    }

    /**
     * Removes the key at index
     * @param index
     * @return
     */
    @Override
    public byte removeIndex(int index) {
        final int keysIndex = indexes2KeysIndexes[index];
        if(keysIndex != DEFAULT_INDEX) { //index has a value
            byte key = keys[keysIndex];
            size--;
            shiftKeys(keysIndex);
            used[keysIndex] = false;
            indexes2KeysIndexes[index] = DEFAULT_INDEX;
            keysIndexes2Indexes[keysIndex] = DEFAULT_KEY;
            keys[keysIndex] = DEFAULT_KEY;
            return key;
        }
        return DEFAULT_KEY;
    }

    /**
     * Shifts left entries with the specified hash code, starting at the specified position,
     * and empties the resulting free entry.
     *
     * @param keysIndex a starting position
     */
    protected final void shiftKeys(int keysIndex) {
        //Shift entries with the same hash.
        int last, slot;
        for(; ;) {
            keysIndex = ( (last = keysIndex) + 1) & mask;
            while(used[keysIndex]) {
                slot = HashCommon.murmurHash3(keys[keysIndex]) & mask;
                if(last <= keysIndex ? last >= slot || slot > keysIndex : last >= slot && slot > keysIndex) break;
                keysIndex = (keysIndex + 1) & mask;
            }
            if(!used[keysIndex]) break;
            keys[last] =  keys[keysIndex];
        }
        assert (used[last]);
        used[last] = false;

        int index = keysIndexes2Indexes[last];
        keys[last] = DEFAULT_KEY;
        indexes2KeysIndexes[index] = DEFAULT_INDEX;
        keysIndexes2Indexes[last] = DEFAULT_KEY;
        if(nextIndex == index) {
            nextIndex = getOccupiedIndexBefore(index);
        }
        if(nextIndex == index) {
            minIndex = getOccupiedIndexBefore(index);
        }
        if(maxIndex == index) {
            maxIndex = getOccupiedIndexBefore(index);
        }
    }

    /**
     * Returns the index before the passed in parameter that is not equal to DEFAULT_INDEX.
     * @param index
     * @return
     */
    private int getOccupiedIndexBefore(int index) {
        //find index before
        for(int i=index-1; i>=0; i--) {
            if(indexes2KeysIndexes[i] != DEFAULT_INDEX) {
                return i;
            }
        }
        //didn't find an index befor, now go forward
        for(int i=index+1; i< indexes2KeysIndexes.length; i++) {
            if(indexes2KeysIndexes[i] != DEFAULT_INDEX) {
                return i;
            }
        }
        return DEFAULT_INDEX;
    }

    /**
     * Removes the key.
     * @param key
     * @return
     */
    @Override
    public int remove(byte key) {
        //the starting point.
        int keysIndex = HashCommon.murmurHash3(key) & mask;
        //there's always an unused entry.
        while(used[keysIndex]) {
            if(key == keys[keysIndex]) {//found key to remove
                int index = keysIndexes2Indexes[keysIndex];
                assert (index != DEFAULT_INDEX);
                assert (indexes2KeysIndexes[index] == keysIndex);
                removeIndex(index);
                return index;
            }
            keysIndex = (keysIndex +1) & mask;
        }
        return DEFAULT_INDEX;
    }

    @Override
    public int getLastIndex() {
        return lastIndex;
    }

    @Override
    public void initializeEntryIterator(EntryIterator destination) {
        destination.reset(indexes2KeysIndexes);
    }

    @Override
    public EntryIterator generateEntryIterator() {
        return new EntryIteratorImpl();
    }

    @Override
    public void initializeKeysIterator(ByteKeysIterator byteKeysIterator) {
        byteKeysIterator.reset();;
    }

    @Override
    public ByteKeysIterator generateKeysIterator() {
        return new ByteKeysIteratorImpl();
    }

    protected final EntryIteratorImpl internalEntryIterator = new EntryIteratorImpl();

    class ByteKeysIteratorImpl implements ByteKeysIterator {
        @Override
        public boolean hasNext() {
            return internalEntryIterator.hasNext();
        }

        @Override
        public byte next() {
            final int index = internalEntryIterator.nextEntry();
            return getKey(index);
        }

        @Override
        public void reset() {
            initializeEntryIterator(internalEntryIterator);
        }
    }


}
