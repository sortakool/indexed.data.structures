package com.rsm.collections.sets2;

import com.rsm.collections.IndexedCollection;
import com.rsm.collections.IndexedCollectionListener;
import com.rsm.collections.sets.IndexedByteOpenHashSet;
import com.rsm.collections.sets.IndexedByteSet;
import gnu.trove.set.hash.TByteHashSet;
import it.unimi.dsi.fastutil.bytes.Byte2IntMap;
import it.unimi.dsi.fastutil.bytes.Byte2IntOpenHashMap;
import junit.framework.Assert;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.Map;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Raymond on 12/10/13.
 */
public class TIndexedByteOpenHashSetTest {

    protected Logger log = LogManager.getLogger(getClass());

    @Test
    public void testAdd_Get_Remove() throws Exception {
        short expected = IndexedByteSet.MAX_VALUES;
        float loadFactor = 0.8f;
        doAdd_Get_Remove(expected, loadFactor);
    }

    protected void doAdd_Get_Remove(short expected, float loadFactor) throws Exception {
        StringBuilder sb = new StringBuilder(1024);
        TIndexedByteOpenHashSet set = new TIndexedByteOpenHashSet(expected, loadFactor, Byte.MIN_VALUE);
        TByteHashSet set2 = new TByteHashSet(expected, loadFactor);

        int indexCounter = 0;
        Byte2IntOpenHashMap key2Index = new Byte2IntOpenHashMap(IndexedByteSet.MAX_VALUES*2, 1.0f);
        Assert.assertEquals(0, set.size());
        //add and get
        for(short shortKey=Byte.MIN_VALUE; shortKey<=Byte.MAX_VALUE; shortKey++) {
            boolean willResize = false;
            byte key = (byte)shortKey;
            key2Index.put(key, indexCounter);
//            if(indexCounter == (set.getMaxFill()-1)) {
//                printSet(sb, set, "Before Resize", null);
//                willResize = true;
//            }
            int index = set.add(key);
            int reinsertIndex = set.add(key);
            boolean added = set2.add(key);
            assertTrue(added);
//            if(willResize) {
//                printSet(sb, set, "After Resize", null);
//            }
            log.info("add: [key="+key+"][index="+index+"]");
            Assert.assertEquals(shortKey + ": Returned index for key '" + key + "'+ should be " + indexCounter, indexCounter, index);
            Assert.assertEquals(shortKey + ": Reinserted index for key '" + key + "'+ should be " + indexCounter, index, reinsertIndex);
            int retrievedIndex = set.getIndex(key);
            Assert.assertEquals("Returned index for key '" + key + "' should be " + indexCounter, indexCounter, retrievedIndex);
//            int lastIndex = set.getLastIndex();
//            Assert.assertEquals("Last index for key '" + key + "' should be " + indexCounter, indexCounter, lastIndex);
            byte retrievedKey = set.getKey(index);
            Assert.assertEquals("Key for index " + index + " should be " + key, key, retrievedKey);
            indexCounter++;
            Assert.assertEquals(indexCounter, set.size());
        }
        Assert.assertEquals(IndexedByteSet.MAX_VALUES, indexCounter-1);
        Assert.assertEquals(indexCounter, set.size());
        Assert.assertEquals(key2Index.size(), set.size());

        int currentSize;
        int i=0;

        log.info("-------------------------ReAdding (To Remove By Key)-------------------------");

        //re-add to make sure the indexes remain the same
        for(Map.Entry<Byte, Integer> byteIntegerEntry : key2Index.entrySet()) {
            Byte2IntMap.Entry next = (Byte2IntMap.Entry)byteIntegerEntry;
            byte key = next.getByteKey();
            int index = next.getIntValue();
            final int addedIndex = set.add(key);
            Assert.assertEquals("Returned index for re-adding key '" + key + "' should be " + index + "'", index, addedIndex);
        }
        Assert.assertEquals(IndexedByteSet.MAX_VALUES, indexCounter-1);
        Assert.assertEquals(indexCounter, set.size());
        Assert.assertEquals(key2Index.size(), set.size());

        //removal by key
        currentSize = set.size();
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
            Assert.assertEquals(currentSize, set.size());
            final byte retrievedKey = set.getKey(index);
            Assert.assertEquals("key " + key + " at index " + index + " should be removed", IndexedByteSet.DEFAULT_KEY, retrievedKey);
            int retrievedIndex = set.getIndex(key);
            Assert.assertEquals("index " + index + " for key " + key + " should be removed", IndexedByteSet.DEFAULT_INDEX, retrievedIndex);
            i++;
        }
        Assert.assertEquals(0, set.size());

        log.info("-------------------------ReAdding (To Remove By Index)-------------------------");

        indexCounter = 0;
        key2Index.clear();
        set.clear();
        Assert.assertEquals(0, set.size());
        //add and get
        for(short shortKey=Byte.MIN_VALUE; shortKey<=Byte.MAX_VALUE; shortKey++) {
            boolean willResize = false;
            byte key = (byte)shortKey;
            key2Index.put(key, indexCounter);
//            if(indexCounter == (set.getMaxFill()-1)) {
//                printSet(sb, set, "Before Resize", null);
//                willResize = true;
//            }
            int index = set.add(key);
            int reinsertIndex = set.add(key);
            boolean added = set2.add(key);
            assertFalse(added);
//            if(willResize) {
//                printSet(sb, set, "After Resize", null);
//            }
            log.info("add: [key="+key+"][index="+index+"]");
            Assert.assertEquals(shortKey + ": Returned index for key '" + key + "'+ should be " + indexCounter, indexCounter, index);
            Assert.assertEquals(shortKey + ": Reinserted index for key '" + key + "'+ should be " + indexCounter, index, reinsertIndex);
            int retrievedIndex = set.getIndex(key);
            Assert.assertEquals("Returned index for key '" + key + "' should be " + indexCounter, indexCounter, retrievedIndex);
//            int lastIndex = set.getLastIndex();
//            Assert.assertEquals("Last index for key '" + key + "' should be " + indexCounter, indexCounter, lastIndex);
            byte retrievedKey = set.getKey(index);
            Assert.assertEquals("Key for index " + index + " should be " + key, key, retrievedKey);
            indexCounter++;
            Assert.assertEquals(indexCounter, set.size());
        }
        Assert.assertEquals(IndexedByteSet.MAX_VALUES, indexCounter-1);
        Assert.assertEquals(indexCounter, set.size());
        Assert.assertEquals(key2Index.size(), set.size());


        //removal by index
        currentSize = set.size();
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
            Assert.assertEquals(currentSize, set.size());
            final byte retrievedKey = set.getKey(index);
            Assert.assertEquals("key " + key + " at index " + index + " should be removed", IndexedByteSet.DEFAULT_KEY, retrievedKey);
            int retrievedIndex = set.getIndex(key);
            Assert.assertEquals("index " + index + " for key " + key + " should be removed", IndexedByteSet.DEFAULT_INDEX, retrievedIndex);
            i++;
        }
        Assert.assertEquals(0, set.size());
    }
}
