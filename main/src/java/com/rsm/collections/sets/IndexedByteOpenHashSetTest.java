package com.rsm.collections.sets;

import com.rsm.collections.IndexedCollection;
import com.rsm.collections.IndexedCollectionListener;
import it.unimi.dsi.fastutil.bytes.Byte2IntMap;
import it.unimi.dsi.fastutil.bytes.Byte2IntOpenHashMap;
import junit.framework.Assert;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.Map;

/**
 * Created by Raymond on 12/5/13.
 */
public class IndexedByteOpenHashSetTest {

    protected Logger log = LogManager.getLogger(getClass());

    @Test
    public void testAdd_Get_Remove() throws Exception {
        short expected = IndexedByteSet.MAX_VALUES;
        float loadFactor = 0.8f;
        doAdd_Get_Remove(expected, loadFactor);
    }

    @Test
    public void testAll_Add_Get_Remove() throws Exception {
//        float[] loadFactors = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f};
        float[] loadFactors = {0.1f};
        for(short i=1; i<= IndexedByteSet.MAX_VALUES; i++) {
            for(float loadFactor : loadFactors) {
                doAdd_Get_Remove(i, loadFactor);
            }
        }
    }

    protected void doAdd_Get_Remove(short expected, float loadFactor) throws Exception {
        StringBuilder sb = new StringBuilder(1024);
        IndexedByteOpenHashSet set = new IndexedByteOpenHashSet(expected, loadFactor);
        set.addCollectionListeners(new IndexedCollectionListener() {
            @Override
            public void onResize(IndexedCollection collection, int oldSize, int newSize) {
                log.info("onResize:[oldSize="+oldSize+"][newSize="+newSize+"]");
            }
        });

        int indexCounter = 0;
        Byte2IntOpenHashMap key2Index = new Byte2IntOpenHashMap(IndexedByteSet.MAX_VALUES*2, 1.0f);
        Assert.assertEquals(0, set.getSize());
        //add and get
        for(short i=Byte.MIN_VALUE; i<=Byte.MAX_VALUE; i++) {
            boolean willResize = false;
            byte key = (byte)i;
            key2Index.put(key, indexCounter);
            if(indexCounter == (set.getMaxFill()-1)) {
                printSet(sb, set, "Before Resize", null);
                willResize = true;
            }
            int index = set.add(key);
            int reInsertedIndex = set.add(key);
            if(willResize) {
                printSet(sb, set, "After Resize", null);
            }
            log.info("add: [key="+key+"][index="+index+"]");
            Assert.assertEquals("Returned index for key '" + key + "'+ should be " + indexCounter, indexCounter, index);
            int retrievedIndex = set.getIndex(key);
            Assert.assertEquals("Returned index for key '" + key + "' should be " + indexCounter, indexCounter, retrievedIndex);
            Assert.assertEquals("Reinserted index for key '" + key + "' should be " + retrievedIndex, retrievedIndex, reInsertedIndex);
            int lastIndex = set.getLastIndex();
            Assert.assertEquals("Last index for key '" + key + "' should be " + indexCounter, indexCounter, lastIndex);
            byte retrievedKey = set.getKey(index);
            Assert.assertEquals("Key for index " + index + " should be " + key, key, retrievedKey);
            indexCounter++;
            Assert.assertEquals(indexCounter, set.getSize());
        }
        Assert.assertEquals(IndexedByteSet.MAX_VALUES, indexCounter-1);
        Assert.assertEquals(indexCounter, set.getSize());
        Assert.assertEquals(key2Index.size(), set.getSize());

        log.info("-------------------------ReAdding (To Remove By Index)-------------------------");

        //re-add to make sure the indexes remain the same
        for(Map.Entry<Byte, Integer> byteIntegerEntry : key2Index.entrySet()) {
            Byte2IntMap.Entry next = (Byte2IntMap.Entry)byteIntegerEntry;
            byte key = next.getByteKey();
            int index = next.getIntValue();
            final int addedIndex = set.add(key);
            Assert.assertEquals("Returned index for re-adding key '" + key + "' should be " + index + "'", index, addedIndex);
        }
        Assert.assertEquals(IndexedByteSet.MAX_VALUES, indexCounter-1);
        Assert.assertEquals(indexCounter, set.getSize());
        Assert.assertEquals(key2Index.size(), set.getSize());

        int currentSize;
        int i=0;

//        //remove
//        tempCheck(set, "before removal");

        //removal by key
        currentSize = set.getSize();
        log.info("-------------------------Removing By Key-------------------------");
        i=0;
        for(Map.Entry<Byte, Integer> byteIntegerEntry : key2Index.entrySet()) {
            Byte2IntMap.Entry next = (Byte2IntMap.Entry)byteIntegerEntry;
            byte key = next.getByteKey();
            int index = next.getIntValue();
            log.info("removing: [key="+key + "][index="+index+"]");
//            printSet(sb, set, "Before.removeIndex("+index+"): ", null);

            int removedIndex = set.remove(key);
//            printSet(sb, set, "After.removeIndex("+index+"): ", null);
            Assert.assertEquals(i + " removedIndex " + removedIndex + " should be " + index + " for key " + key, index, removedIndex);
            currentSize--;
            Assert.assertEquals(currentSize, set.getSize());
            final byte retrievedKey = set.getKey(index);
            Assert.assertEquals("key " + key + " at index " + index + " should be removed", IndexedByteSet.DEFAULT_KEY, retrievedKey);
            int retrievedIndex = set.getIndex(key);
            Assert.assertEquals("index " + index + " for key " + key + " should be removed", IndexedByteSet.DEFAULT_INDEX, retrievedIndex);
//            tempCheck(set, "[key="+key+"][index="+index+"]");
            i++;
        }
        Assert.assertEquals(0, set.getSize());

        //removal by index
        currentSize = set.getSize();
        log.info("-------------------------Removing By Index-------------------------");
        i=0;
        for(Map.Entry<Byte, Integer> byteIntegerEntry : key2Index.entrySet()) {
            Byte2IntMap.Entry next = (Byte2IntMap.Entry)byteIntegerEntry;
            byte key = next.getByteKey();
            int index = next.getIntValue();
            log.info("removing: [key="+key + "][index="+index+"]");
//            printSet(sb, set, "Before.removeIndex("+index+"): ", null);
            byte removedKey = set.removeIndex(index);
//            printSet(sb, set, "After.removeIndex("+index+"): ", null);
            Assert.assertEquals(i + " removedKey " + removedKey + " should be " + key + " for index " + index, key, removedKey);
            currentSize--;
            Assert.assertEquals(currentSize, set.getSize());
            final byte retrievedKey = set.getKey(index);
            Assert.assertEquals("key " + key + " at index " + index + " should be removed", IndexedByteSet.DEFAULT_KEY, retrievedKey);
            int retrievedIndex = set.getIndex(key);
            Assert.assertEquals("index " + index + " for key " + key + " should be removed", IndexedByteSet.DEFAULT_INDEX, retrievedIndex);
            i++;
        }
        Assert.assertEquals(0, set.getSize());

    }

//    private void tempCheck(IndexedByteOpenHashSet set, String message) {
//        int tempIndex = set.getIndex((byte)119);
//        assert(tempIndex == 247) : message;
//    }

    @Test
    public void testOneKeyAtATime() {
        float loadFactor = 0.8f;
        IndexedByteOpenHashSet set = new IndexedByteOpenHashSet(IndexedByteSet.MAX_VALUES, loadFactor);
        set.addCollectionListeners(new IndexedCollectionListener() {
            @Override
            public void onResize(IndexedCollection collection, int oldSize, int newSize) {
                log.info("onResize:[oldSize="+oldSize+"][newSize="+newSize+"]");
            }
        });

        int indexCounter = 0;
        Byte2IntOpenHashMap key2Index = new Byte2IntOpenHashMap(IndexedByteSet.MAX_VALUES*2, 1.0f);
        Assert.assertEquals(0, set.getSize());
        //add and get
        for(short i=Byte.MIN_VALUE; i<=Byte.MAX_VALUE; i++) {
            boolean willResize = false;
            byte key = (byte)i;
            int index = set.add(key);
            log.info("add: [key="+key+"][index="+index+"]");
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Returned index for key '" + key + "'+ should be " + 0, 0, index);
            int retrievedIndex = set.getIndex(key);
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Returned index for key '" + key + "' should be " + 0, 0, retrievedIndex);
            int lastIndex = set.getLastIndex();
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Last index for key '" + key + "' should be " + 0, 0, lastIndex);
            byte retrievedKey = set.getKey(index);
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Key for index " + index + " should be " + key, key, retrievedKey);

            byte removedKeyAtIndex = set.removeIndex(index);
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Removed Key for index " + index + " should be " + key, key, removedKeyAtIndex);

            int newIndex = set.add(key);
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Readded index for key '" + key + "'+ should be " + index, index, newIndex);

            int removedIndex = set.remove(key);
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Removed index for key '" + key + "' should be " + index, index, removedIndex);

            indexCounter++;
//            Assert.assertEquals(indexCounter, set.getSize());
        }
    }

    @Test
    public void testOneKeyAtATime2() {
        float loadFactor = 0.8f;
        IndexedByteOpenHashSet set = new IndexedByteOpenHashSet(IndexedByteSet.MAX_VALUES, loadFactor);
        set.addCollectionListeners(new IndexedCollectionListener() {
            @Override
            public void onResize(IndexedCollection collection, int oldSize, int newSize) {
                log.info("onResize:[oldSize="+oldSize+"][newSize="+newSize+"]");
            }
        });

        int max = 100;
        int maxLimit = Math.min(Byte.MAX_VALUE, max);

        int indexCounter = 0;
        Byte2IntOpenHashMap key2Index = new Byte2IntOpenHashMap(IndexedByteSet.MAX_VALUES*2, 1.0f);
        Assert.assertEquals(0, set.getSize());



        //add and get
        for(short i=Byte.MIN_VALUE; i<=maxLimit; i++) {
            boolean willResize = false;
            byte key = (byte)i;
            int index = set.add(key);
            log.info("add: [key="+key+"][index="+index+"]");
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Returned index for key '" + key + "'+ should be " + 0, 0, index);
            int retrievedIndex = set.getIndex(key);
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Returned index for key '" + key + "' should be " + 0, 0, retrievedIndex);
            int lastIndex = set.getLastIndex();
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Last index for key '" + key + "' should be " + 0, 0, lastIndex);
            byte retrievedKey = set.getKey(index);
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Key for index " + index + " should be " + key, key, retrievedKey);

            byte removedKeyAtIndex = set.removeIndex(index);
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Removed Key for index " + index + " should be " + key, key, removedKeyAtIndex);

            int newIndex = set.add(key);
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Readded index for key '" + key + "'+ should be " + index, index, newIndex);

            int removedIndex = set.remove(key);
            Assert.assertEquals("[indexCounter="+indexCounter+"]" + "[key="+key+"] " + "Removed index for key '" + key + "' should be " + index, index, removedIndex);

            indexCounter++;
//            Assert.assertEquals(indexCounter, set.getSize());
        }
    }

    private void printSet(StringBuilder sb, IndexedByteSet set, String prefix, String suffix) {
        sb.setLength(0);
        if(prefix != null) {
            sb.append(prefix);
        }
        set.print(sb);
        if(suffix != null) {
            sb.append(suffix);
        }
        log.info(sb.toString());
    }
}
