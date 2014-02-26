package com.rsm.collections.sets2;

import gnu.trove.impl.HashFunctions;
import gnu.trove.impl.hash.TByteHash;

import java.util.Arrays;

/**
 * An open addressed set implementation for byte primitives.
 *
 * @see gnu.trove.set.hash.TByteHashSet
 * Created by Raymond on 12/9/13.
 */
public class TIndexedByteOpenHashSet extends TByteHash  {

    public static final int DEFAULT_HANDLE = -1;
    public static final int DEFAULT_INDEX = -2;

    public transient int[] _handles2Indexes;
    public transient int[] _indexes2Handles;

    protected int nextHandle = DEFAULT_HANDLE;
    protected int minHandle = DEFAULT_HANDLE;
    protected int maxHandle = DEFAULT_HANDLE;


    /**
     * Creates a new <code>TByteHashSet</code> instance with the default
     * capacity and load factor.
     */
    public TIndexedByteOpenHashSet() {
        super();
    }


    /**
     * Creates a new <code>TByteHashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public TIndexedByteOpenHashSet( int initialCapacity ) {
        super( initialCapacity );
        reset();

        //disable compaction
        _autoCompactionFactor = 0;
    }


    /**
     * Creates a new <code>TIntHash</code> instance with a prime
     * value at or near the specified capacity and load factor.
     *
     * @param initialCapacity used to find a prime capacity for the table.
     * @para-m load_factor used to calculate the threshold over which
     * rehashing takes place.
     */
    public TIndexedByteOpenHashSet( int initialCapacity, float load_factor ) {
        super(initialCapacity, load_factor);
        reset();

        //disable compaction
        _autoCompactionFactor = 0;
    }


    /**
     * Creates a new <code>TByteHashSet</code> instance with a prime
     * capacity equal to or greater than <tt>initial_capacity</tt> and
     * with the specified load factor.
     *
     * @param initial_capacity an <code>int</code> value
     * @param load_factor a <code>float</code> value
     * @param no_entry_value a <code>byte</code> value that represents null.
     */
    public TIndexedByteOpenHashSet( int initial_capacity, float load_factor,
                         byte no_entry_value ) {
        super( initial_capacity, load_factor, no_entry_value );
        //noinspection RedundantCast
        if ( no_entry_value != ( byte ) 0 ) {
            Arrays.fill(_set, no_entry_value);
        }

        reset();

        //disable compaction
        _autoCompactionFactor = 0;
    }

    @Override
    public void clear() {
        super.clear();
        reset();
    }

    private void reset() {
        resetHandles();
        resetArrays();
    }

    private void resetArrays() {
        Arrays.fill(_handles2Indexes, DEFAULT_INDEX);
        Arrays.fill(_indexes2Handles, DEFAULT_HANDLE);
    }

    private void resetHandles() {
        nextHandle = DEFAULT_HANDLE;
        minHandle = DEFAULT_HANDLE;
        maxHandle = DEFAULT_HANDLE;
    }

    @Override
    protected int setUp( int initialCapacity ) {
        int capacity;

        capacity = super.setUp( initialCapacity );
        _handles2Indexes = new int[capacity];
        _indexes2Handles = new int[capacity];

        resetArrays();

        return capacity;
    }

    boolean isRehash = false;

//    @Override
//    /** {@inheritDoc} */
//    protected void rehash(int newCapacity) {
//        isRehash = true;
//        int oldCapacity = _set.length;
//
//        byte oldSet[] = _set;
//        byte oldStates[] = _states;
//        int oldIndexes2Handles[] = _indexes2Handles;
//        int oldHandles2Indexes[] = _handles2Indexes;
//
//        _set = new byte[newCapacity];
//        _states = new byte[newCapacity];
//        _indexes2Handles = new int[newCapacity];
//        _handles2Indexes = new int[newCapacity];
//        reset();
////        resetArrays();
//
//        for ( int index = oldCapacity; index-- > 0; ) {
//            if( oldStates[index] == FULL ) {
//                int handle = oldIndexes2Handles[index];
//                assert(oldHandles2Indexes[handle] == index);
//                byte key = oldSet[index];
//                int newIndex = insertKey(key);
//                assert(_set[newIndex] == key);
//                assert(_states[newIndex] == FULL);
//
//                if(_handles2Indexes[handle] != newIndex) {
//                    _handles2Indexes[handle] = newIndex;
//                    _indexes2Handles[newIndex] =  handle;
//                }
////                assert(_indexes2Handles[newIndex] != DEFAULT_HANDLE);
////                assert(_handles2Indexes[_indexes2Handles[newIndex]] != DEFAULT_INDEX);
//            }
//        }
//        isRehash = false;
//    }

    @Override
    /** {@inheritDoc} */
    protected void rehash(int newCapacity) {
        isRehash = true;
        int oldCapacity = _set.length;

        byte oldSet[] = _set;
        byte oldStates[] = _states;
        int oldIndexes2Handles[] = _indexes2Handles;
        int oldHandles2Indexes[] = _handles2Indexes;

        int oldMinHandle = minHandle;
        int oldMaxHandle = maxHandle;
        int oldNextHandle = nextHandle;

        _set = new byte[newCapacity];
        _states = new byte[newCapacity];
        _indexes2Handles = new int[newCapacity];
        _handles2Indexes = new int[newCapacity];


        reset();
//        resetArrays();

        for ( int handle = oldMinHandle; handle <= oldMaxHandle; handle++) {
            int index = oldHandles2Indexes[handle];
            if( oldStates[index] == FULL ) {
                assert( oldIndexes2Handles[index] == handle);
                assert( oldHandles2Indexes[handle] == index );

                byte key = oldSet[index];
                int newIndex = insertKey(key);
                assert(_set[newIndex] == key);
                assert(_states[newIndex] == FULL);

//                if(_handles2Indexes[handle] != newIndex) {
//                    _handles2Indexes[handle] = newIndex;
//                    _indexes2Handles[newIndex] =  handle;
//                }
//                assert(_indexes2Handles[newIndex] != DEFAULT_HANDLE);
//                assert(_handles2Indexes[_indexes2Handles[newIndex]] != DEFAULT_INDEX);
            }
        }
        isRehash = false;
    }

    private int getNextFreeHandle() {
//        int tempHandle = nextHandle;
        for(int handle=nextHandle+1; handle< _handles2Indexes.length; handle++) {
            if(_handles2Indexes[handle] == DEFAULT_INDEX) {
                return handle;
            }
        }
        for(int handle=minHandle; handle<nextHandle; handle++) {
            if(_handles2Indexes[handle] == DEFAULT_INDEX) {
                return handle;
            }
        }
        throw new RuntimeException("Unable to find next index. Where [minHandle="+minHandle+"][nextHandle="+nextHandle+"][maxHandle="+maxHandle+"]");
    }

    public int add( byte val ) {
        int index = insertKey(val);

        if ( index < 0 ) {
            // already present in set, nothing to add
            int handle = _indexes2Handles[Math.abs(index)-1];
            assert (handle != DEFAULT_HANDLE);
            return handle;
        }

        postInsertHook( consumeFreeSlot );

        int handle = _indexes2Handles[index];
        if(!isRehash) {
//            assert (handle == nextHandle) : "add[key="+val+"][index="+index+"][handle="+handle+"][nextHandle="+nextHandle+"]";
        }
        return handle;            // yes, we added something
    }

//    public int getIndex(byte val) {
//        int index = getKeyIndex(val);
//        if(index < 0) {
//            int adjustedIndex = Math.abs(index) - 1;
//            int handle = _indexes2Handles[adjustedIndex];
//            assert (handle != DEFAULT_HANDLE);
//            assert(_handles2Indexes[handle] == adjustedIndex);
//            assert(_set[adjustedIndex] == val);
//            assert(_states[adjustedIndex] == FULL);
//            return handle;
//        }
//
//        return DEFAULT_HANDLE;
//    }

    public int getIndex(byte val) {
        int index = index(val);
        if(index >= 0) {
            int handle = _indexes2Handles[index];
            assert (handle != DEFAULT_HANDLE);
            assert(_handles2Indexes[handle] == index);
            assert(_set[index] == val);
            assert(_states[index] == FULL);
            return handle;
        }

        return DEFAULT_HANDLE;
    }

//    public int getKeyIndex(byte val) {
//        int hash, index;
//
//        hash = HashFunctions.hash(val) & 0x7fffffff;
//        index = hash % _states.length;
//        byte state = _states[index];
//
//        if ((state == FULL) && (_set[index] == val)) {
//            return -index - 1;   // already stored
//        }
//
//        //mimic logic of com.rsm.collections.sets2.TIndexedByteOpenHashSet.insertKeyRehash without insert
//
//        // compute the double hash
//        final int length = _set.length;
//        int probe = 1 + (hash % (length - 2));
//        final int loopIndex = index;
//        int firstRemoved = -1;
//
//        /**
//         * Look until FREE slot or we start to loop
//         */
//        do {
//            // Identify first removed slot
//            if (state == REMOVED && firstRemoved == -1)
//                firstRemoved = index;
//
//            index -= probe;
//            if (index < 0) {
//                index += length;
//            }
//            state = _states[index];
//
////            // A FREE slot stops the search
////            if (state == FREE) {
////                if (firstRemoved != -1) {
////                    insertKeyAt(firstRemoved, val);
////                    return firstRemoved;
////                } else {
////                    consumeFreeSlot = true;
////                    insertKeyAt(index, val);
////                    return index;
////                }
////            }
//
//            if (state == FULL && _set[index] == val) {
//                return -index - 1;
//            }
//
//            // Detect loop
//        } while (index != loopIndex);
//
////        // We inspected all reachable slots and did not find a FREE one
////        // If we found a REMOVED slot we return the first one found
////        if (firstRemoved != -1) {
////            insertKeyAt(firstRemoved, val);
////            return firstRemoved;
////        }
////
////        Can a resizing strategy be found that resizes the set?
//        //(val, index, hash, state);
//        throw new IllegalStateException("Unable to getIndex for [key="+val+"][index="+index+"][hash="+hash+"][state="+state+"]");
//    }

    /**
     * Locates the index at which <tt>val</tt> can be inserted.  if
     * there is already a value equal()ing <tt>val</tt> in the set,
     * returns that value as a negative integer.
     *
     * @param val an <code>byte</code> value
     * @return an <code>int</code> value
     */
    protected int insertKey( byte val ) {
        int hash, index;

        hash = HashFunctions.hash(val) & 0x7fffffff;
        index = hash % _states.length;
        byte state = _states[index];

        consumeFreeSlot = false;

        if (state == FREE) {
            assert (_indexes2Handles[index] == DEFAULT_HANDLE);
            consumeFreeSlot = true;
            insertKeyAt(index, val);

            return index;       // empty, all done
        }

        if (state == FULL && _set[index] == val) {
            return -index - 1;   // already stored
        }

        // already FULL or REMOVED, must probe
        return insertKeyRehash(val, index, hash, state);
    }

    protected int insertKeyRehash(byte val, int index, int hash, byte state) {
        // compute the double hash
        final int length = _set.length;
        int probe = 1 + (hash % (length - 2));
        final int loopIndex = index;
        int firstRemoved = -1;

        /**
         * Look until FREE slot or we start to loop
         */
        do {
            // Identify first removed slot
            if (state == REMOVED && firstRemoved == -1)
                firstRemoved = index;

            index -= probe;
            if (index < 0) {
                index += length;
            }
            state = _states[index];

            // A FREE slot stops the search
            if (state == FREE) {
                if (firstRemoved != -1) {
                    insertKeyAt(firstRemoved, val);
                    return firstRemoved;
                } else {
                    consumeFreeSlot = true;
                    insertKeyAt(index, val);
                    return index;
                }
            }

            if (state == FULL && _set[index] == val) {
                return -index - 1;
            }

            // Detect loop
        } while (index != loopIndex);

        // We inspected all reachable slots and did not find a FREE one
        // If we found a REMOVED slot we return the first one found
        if (firstRemoved != -1) {
            insertKeyAt(firstRemoved, val);
            return firstRemoved;
        }

        // Can a resizing strategy be found that resizes the set?
        throw new IllegalStateException("No free or removed slots available. Key set full?!!");
    }

    protected void insertKeyAt(int index, byte val) {
        assert (_indexes2Handles[index] == DEFAULT_HANDLE);
        nextHandle = getNextFreeHandle();
        minHandle = Math.max(0, Math.min(nextHandle, minHandle));
        maxHandle = Math.max(nextHandle, maxHandle);

        assert (nextHandle != DEFAULT_HANDLE);
        assert (minHandle <= nextHandle);
        assert (nextHandle <= maxHandle);

        _indexes2Handles[index] = nextHandle;
        _handles2Indexes[nextHandle] = index;


        _set[index] = val;  // insert value
        _states[index] = FULL;
    }

    public byte getKey(int handle) {
        int index = _handles2Indexes[handle];
        if(index != DEFAULT_INDEX) {
            byte key = _set[index];
            assert (_states[index] == FULL);
            assert (_indexes2Handles[index] == handle);
            return key;
        }
        return no_entry_value;
    }

    public int remove(byte val) {
        int index = index(val);
        if(index >= 0) {
            int handle = _indexes2Handles[index];
            assert( handle != DEFAULT_HANDLE);
            assert( index == _handles2Indexes[handle]);
            removeAt(index);
            return handle;
        }
        return DEFAULT_HANDLE;
    }

    @Override
    protected void removeAt( int index ) {
        int handle = _indexes2Handles[index];
        _handles2Indexes[handle] = DEFAULT_INDEX;
        _indexes2Handles[index] = DEFAULT_HANDLE;
        super.removeAt(index);
    }

    public byte removeIndex(int handle) {
        int index = _handles2Indexes[handle];
        if(index != DEFAULT_INDEX) {
            byte key = _set[index];
            assert(_states[index] == FULL);
            assert (_indexes2Handles[index] == handle);
            remove(key);
            return key;
        }
        return no_entry_value;
    }


}

