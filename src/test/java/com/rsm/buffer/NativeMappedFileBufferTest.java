package com.rsm.buffer;

import com.rsm.util.ByteUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by rmanaloto on 4/19/14.
 */
public class NativeMappedFileBufferTest {

    private static final Logger log = LogManager.getLogger(NativeMappedFileBufferTest.class);

    private static final Random random = new Random();

    @Test
    public void testByte() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //byte
        byte testByte;
        byte returnedByte;
        for (position = 0; position < initialFileSize * 2; position++) {
            testByte = (byte) random.nextInt(Byte.MAX_VALUE);
            //absolute
            mappedFile.put(position, testByte);
            returnedByte = mappedFile.get(position);
            Assert.assertEquals(testByte, returnedByte);

            //relative
            mappedFile.position(position);
            returnedByte = mappedFile.get();
            Assert.assertEquals(testByte, returnedByte);

            testByte = (byte) random.nextInt(Byte.MAX_VALUE);
            mappedFile.position(position);
            mappedFile.put(testByte);
            mappedFile.position(position);
            returnedByte = mappedFile.get();
            Assert.assertEquals(testByte, returnedByte);
        }
    }

    @Test
    public void testShort() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //short
        short testShort;
        short returnedShort;
        for (position = 0; position < initialFileSize * 2; position++) {
            testShort = (short) random.nextInt(Short.MAX_VALUE);
            //absolute
            mappedFile.putShort(position, testShort, byteOrder);
            returnedShort = mappedFile.getShort(position, byteOrder);
            Assert.assertEquals(testShort, returnedShort);

            //relative
            mappedFile.position(position);
            returnedShort = mappedFile.getShort(byteOrder);
            Assert.assertEquals(testShort, returnedShort);

            testShort = (short) random.nextInt(Short.MAX_VALUE);
            mappedFile.position(position);
            mappedFile.putShort(testShort, byteOrder);
            mappedFile.position(position);
            returnedShort = mappedFile.getShort(byteOrder);
            Assert.assertEquals(testShort, returnedShort);
        }
    }

    @Test
    public void testShort2() throws Exception {
        final int segmentSize = 1;
        final long initialFileSize = ByteUtils.PAGE_SIZE;

        FileSystem fileSystem = FileSystems.getDefault();
        Path path = fileSystem.getPath(System.getProperty("user.home") + "/Downloads/11092013.NASDAQ_ITCH41");
        File file = path.toFile();
        String absolutePath = file.getAbsolutePath();
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(absolutePath, segmentSize, initialFileSize, true, false);

        long position = segmentSize-1;
        short returnedShort;

        mappedFile.position(position);

        returnedShort = mappedFile.getShort(position, ByteOrder.BIG_ENDIAN);
        Assert.assertEquals(5, returnedShort);

        mappedFile.position(position);
        mappedFile.putShort(position, returnedShort, ByteOrder.BIG_ENDIAN);
        returnedShort = mappedFile.getShort(position, ByteOrder.BIG_ENDIAN);
        Assert.assertEquals(5, returnedShort);
    }

    @Test
    public void testInt() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //int
        int testInt;
        int returnedInt;
        for (position = 0; position < initialFileSize * 2; position++) {
            testInt = random.nextInt();
            //absolute
            mappedFile.putInt(position, testInt, byteOrder);
            returnedInt = mappedFile.getInt(position, byteOrder);
            Assert.assertEquals(testInt, returnedInt);

            //relative
            mappedFile.position(position);
            returnedInt = mappedFile.getInt(byteOrder);
            Assert.assertEquals(testInt, returnedInt);

            testInt = random.nextInt();
            mappedFile.position(position);
            mappedFile.putInt(testInt, byteOrder);
            mappedFile.position(position);
            returnedInt = mappedFile.getInt(byteOrder);
            Assert.assertEquals(testInt, returnedInt);
        }
    }

    @Test
    public void testLong() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //long
        long testLong;
        long returnedLong;
        for(position = 0; position<initialFileSize*2; position++) {
            testLong = random.nextLong();
            //absolute
            mappedFile.putLong(position, testLong, byteOrder);
            returnedLong = mappedFile.getLong(position, byteOrder);
            Assert.assertEquals(testLong, returnedLong);

            //relative
            mappedFile.position(position);
            returnedLong = mappedFile.getLong(byteOrder);
            Assert.assertEquals(testLong, returnedLong);

            testLong = random.nextLong();
            mappedFile.position(position);
            mappedFile.putLong(testLong, byteOrder);
            mappedFile.position(position);
            returnedLong = mappedFile.getLong(byteOrder);
            Assert.assertEquals(testLong, returnedLong);
        }
    }

    @Test
    public void testFloat() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //float
        float testFloat;
        float returnedFloat;
        for(position = 0; position<initialFileSize*2; position++) {
            testFloat = random.nextFloat();
            //absolute
            mappedFile.putFloat(position, testFloat, byteOrder);
            returnedFloat = mappedFile.getFloat(position, byteOrder);
            Assert.assertEquals(testFloat, returnedFloat, 0.0f);

            //relative
            mappedFile.position(position);
            returnedFloat = mappedFile.getFloat(byteOrder);
            Assert.assertEquals(testFloat, returnedFloat, 0.0f);

            testFloat = random.nextFloat();
            mappedFile.position(position);
            mappedFile.putFloat(testFloat, byteOrder);
            mappedFile.position(position);
            returnedFloat = mappedFile.getFloat(byteOrder);
            Assert.assertEquals(testFloat, returnedFloat, 0.0f);
        }
    }

    @Test
    public void testDouble() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //float
        double testDouble;
        double returnedDouble;
        for(position = 0; position<initialFileSize*2; position++) {
            testDouble = random.nextDouble();
            //absolute
            mappedFile.putDouble(position, testDouble, byteOrder);
            returnedDouble = mappedFile.getDouble(position, byteOrder);
            Assert.assertEquals(testDouble, returnedDouble, 0.0D);

            //relative
            mappedFile.position(position);
            returnedDouble = mappedFile.getDouble(byteOrder);
            Assert.assertEquals(testDouble, returnedDouble, 0.0D);

            testDouble = random.nextDouble();
            mappedFile.position(position);
            mappedFile.putDouble(testDouble, byteOrder);
            mappedFile.position(position);
            returnedDouble = mappedFile.getDouble(byteOrder);
            Assert.assertEquals(testDouble, returnedDouble, 0.0D);
        }
    }

    @Test
    public void testByteArray() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //ByteBuffer
        int byteBufferCapacity = 32;
        byte[] testByteArray = new byte[byteBufferCapacity];
        byte[] returnedByteArray = new byte[byteBufferCapacity];
        for(position = 0; position<initialFileSize*2; position++) {
//            log.info("[position="+position+"]");
            fillWithRandomBytes(testByteArray);
            //absolute
            mappedFile.putBytes(position, testByteArray);
            mappedFile.getBytes(position, returnedByteArray);
            Assert.assertTrue(Arrays.equals(testByteArray, returnedByteArray));

            //relative
            mappedFile.position(position);
            mappedFile.getBytes(returnedByteArray);
            Assert.assertTrue(Arrays.equals(testByteArray, returnedByteArray));

            fillWithRandomBytes(testByteArray);
            mappedFile.position(position);
            mappedFile.putBytes(testByteArray);
            mappedFile.position(position);
            mappedFile.getBytes(returnedByteArray);
            Assert.assertTrue(Arrays.equals(testByteArray, returnedByteArray));
        }
    }

    @Test
    public void testByteArray2() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //ByteBuffer
        int byteBufferCapacity = (int)mappedFile.segmentSize()+1;
        byte[] testByteBuffer = new byte[byteBufferCapacity];
        byte[] returnedByteBuffer = new byte[byteBufferCapacity];

        position = 0;
        mappedFile.position(position);
        fillWithRandomBytes(testByteBuffer);
        //absolute
        mappedFile.putBytes(position, testByteBuffer);
        Assert.assertEquals(position, mappedFile.position());
        mappedFile.getBytes(position, returnedByteBuffer);
        Assert.assertEquals(position, mappedFile.position());

        for(int i=0; i<byteBufferCapacity; i++) {
            final byte mappedFileValue = mappedFile.get(i);
            final byte testValue = testByteBuffer[i];
            final byte returnedValue = mappedFile.get(i);
            Assert.assertEquals(mappedFileValue, testValue);
            Assert.assertEquals(mappedFileValue, returnedValue);
        }
        Assert.assertTrue(Arrays.equals(testByteBuffer,returnedByteBuffer));
    }

    @Test
    public void testByteBuffer() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //ByteBuffer
        int byteBufferCapacity = 32;
        ByteBuffer testByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        ByteBuffer returnedByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        for(position = 0; position<initialFileSize*2; position++) {
//            log.info("[position="+position+"]");
            testByteBuffer.clear();
            fillWithRandomBytes(testByteBuffer);
            //absolute
            mappedFile.putBytes(position, testByteBuffer);
            Assert.assertEquals(byteBufferCapacity, testByteBuffer.position());
            Assert.assertEquals(byteBufferCapacity, testByteBuffer.limit());
            Assert.assertEquals(0, testByteBuffer.remaining());
            testByteBuffer.flip();
            returnedByteBuffer.clear();
            mappedFile.getBytes(position, returnedByteBuffer);
            Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.position());
            Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.limit());
            Assert.assertEquals(0, returnedByteBuffer.remaining());
            returnedByteBuffer.flip();
            Assert.assertTrue(testByteBuffer.equals(returnedByteBuffer));

            //relative
            mappedFile.position(position);
            returnedByteBuffer.clear();
            mappedFile.getBytes(returnedByteBuffer);
            Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.position());
            Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.limit());
            Assert.assertEquals(0, returnedByteBuffer.remaining());
            returnedByteBuffer.flip();
            Assert.assertTrue(testByteBuffer.equals(returnedByteBuffer));

            testByteBuffer.clear();
            fillWithRandomBytes(testByteBuffer);
            mappedFile.position(position);
            mappedFile.putBytes(testByteBuffer);
            mappedFile.position(position);
            returnedByteBuffer.clear();
            mappedFile.getBytes(returnedByteBuffer);
            Assert.assertTrue(testByteBuffer.equals(returnedByteBuffer));
        }
    }

    @Test
    public void testByteBuffer2() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //ByteBuffer
        int byteBufferCapacity = (int)mappedFile.segmentSize()+1;
        ByteBuffer testByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        ByteBuffer returnedByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);

        position = 0;
        mappedFile.position(position);
        testByteBuffer.clear();
        fillWithRandomBytes(testByteBuffer);
        //absolute
        mappedFile.putBytes(position, testByteBuffer);
        Assert.assertEquals(position, mappedFile.position());
        Assert.assertEquals(byteBufferCapacity, testByteBuffer.position());
        Assert.assertEquals(byteBufferCapacity, testByteBuffer.limit());
        Assert.assertEquals(0, testByteBuffer.remaining());
        testByteBuffer.flip();
        returnedByteBuffer.clear();
        mappedFile.getBytes(position, returnedByteBuffer);
        Assert.assertEquals(position, mappedFile.position());
        Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.position());
        Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.limit());
        Assert.assertEquals(0, returnedByteBuffer.remaining());
        returnedByteBuffer.flip();

        for(int i=0; i<byteBufferCapacity; i++) {
            final byte mappedFileValue = mappedFile.get(i);
            final byte testValue = testByteBuffer.get(i);
            final byte returnedValue = mappedFile.get(i);
            Assert.assertEquals(mappedFileValue, testValue);
            Assert.assertEquals(mappedFileValue, returnedValue);
        }
        Assert.assertEquals(testByteBuffer, returnedByteBuffer);
    }

    @Test
    public void testNativeMappedMemory() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //ByteBuffer
        int byteBufferCapacity = 32;
//        ByteBuffer testByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        NativeMappedMemory testByteBuffer = new NativeMappedMemory(ByteBuffer.allocateDirect(byteBufferCapacity));
//        ByteBuffer returnedByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        NativeMappedMemory returnedByteBuffer = new NativeMappedMemory(ByteBuffer.allocateDirect(byteBufferCapacity));
        for(position = 0; position<initialFileSize*2; position++) {
//            log.info("[position="+position+"]");
            mappedFile.position(position);
            testByteBuffer.clear();
            fillWithRandomBytes(testByteBuffer);
            //absolute
            mappedFile.putBytes(position, testByteBuffer);
            Assert.assertEquals(position, mappedFile.position());
            Assert.assertEquals(byteBufferCapacity, testByteBuffer.position());
            Assert.assertEquals(byteBufferCapacity, testByteBuffer.limit());
            Assert.assertEquals(0, testByteBuffer.remaining());
            testByteBuffer.flip();
            returnedByteBuffer.clear();
            mappedFile.getBytes(position, returnedByteBuffer);
            Assert.assertEquals(position, mappedFile.position());
            Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.position());
            Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.limit());
            Assert.assertEquals(0, returnedByteBuffer.remaining());
            returnedByteBuffer.flip();
            Assert.assertEquals(testByteBuffer, returnedByteBuffer);
//            assertNativeMappedFileBuffers(mappedFile, position+1, testByteBuffer, returnedByteBuffer);

            //relative
            mappedFile.position(position);
            returnedByteBuffer.clear();
            mappedFile.getBytes(returnedByteBuffer);
            Assert.assertEquals(position+returnedByteBuffer.capacity(), mappedFile.position());
            Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.position());
            Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.limit());
            Assert.assertEquals(0, returnedByteBuffer.remaining());
            returnedByteBuffer.flip();
            Assert.assertEquals(testByteBuffer, returnedByteBuffer);
//            assertNativeMappedFileBuffers(mappedFile, position+1, testByteBuffer, returnedByteBuffer);

            testByteBuffer.clear();
            fillWithRandomBytes(testByteBuffer);
            mappedFile.position(position);
            mappedFile.putBytes(testByteBuffer);
            Assert.assertEquals(position+testByteBuffer.capacity(), mappedFile.position());
            Assert.assertEquals(byteBufferCapacity, testByteBuffer.position());
            Assert.assertEquals(byteBufferCapacity, testByteBuffer.limit());
            Assert.assertEquals(0, testByteBuffer.remaining());
            mappedFile.position(position);
            returnedByteBuffer.clear();
            mappedFile.getBytes(returnedByteBuffer);
            Assert.assertEquals(position+returnedByteBuffer.capacity(), mappedFile.position());
            Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.position());
            Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.limit());
            Assert.assertEquals(0, returnedByteBuffer.remaining());
            Assert.assertEquals(testByteBuffer, returnedByteBuffer);
//            assertNativeMappedFileBuffers(mappedFile, position+1, testByteBuffer, returnedByteBuffer);
        }
    }

    @Test
    public void testNativeMappedMemoryAbsolute() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //ByteBuffer
        long byteBufferCapacity = mappedFile.segmentSize()+1;
//        ByteBuffer testByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        NativeMappedMemory testByteBuffer = new NativeMappedMemory(ByteBuffer.allocateDirect((int)byteBufferCapacity));
//        ByteBuffer returnedByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        NativeMappedMemory returnedByteBuffer = new NativeMappedMemory(ByteBuffer.allocateDirect((int)byteBufferCapacity));

        position = 0;
        mappedFile.position(position);
        testByteBuffer.clear();
        fillWithRandomBytes(testByteBuffer);
        //absolute
        mappedFile.putBytes(position, testByteBuffer);
        Assert.assertEquals(position, mappedFile.position());
        Assert.assertEquals(byteBufferCapacity, testByteBuffer.position());
        Assert.assertEquals(byteBufferCapacity, testByteBuffer.limit());
        Assert.assertEquals(0, testByteBuffer.remaining());
        testByteBuffer.flip();
        returnedByteBuffer.clear();
        mappedFile.getBytes(position, returnedByteBuffer);
        Assert.assertEquals(position, mappedFile.position());
        Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.position());
        Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.limit());
        Assert.assertEquals(0, returnedByteBuffer.remaining());
        returnedByteBuffer.flip();

//        assertNativeMappedFileBuffers(mappedFile, byteBufferCapacity, testByteBuffer, returnedByteBuffer);
        Assert.assertEquals(testByteBuffer, returnedByteBuffer);
    }

    @Test
    public void testNativeMappedMemoryRelative() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //ByteBuffer
        long byteBufferCapacity = mappedFile.segmentSize()+1;
//        ByteBuffer testByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        NativeMappedMemory testByteBuffer = new NativeMappedMemory(ByteBuffer.allocateDirect((int)byteBufferCapacity));
//        ByteBuffer returnedByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        NativeMappedMemory returnedByteBuffer = new NativeMappedMemory(ByteBuffer.allocateDirect((int)byteBufferCapacity));

        position = 0;
        mappedFile.position(position);
        testByteBuffer.clear();
        fillWithRandomBytes(testByteBuffer);
        //absolute
        mappedFile.putBytes(testByteBuffer);
        Assert.assertEquals(byteBufferCapacity, mappedFile.position());
        Assert.assertEquals(byteBufferCapacity, testByteBuffer.position());
        Assert.assertEquals(byteBufferCapacity, testByteBuffer.limit());
        Assert.assertEquals(0, testByteBuffer.remaining());
        testByteBuffer.flip();
        returnedByteBuffer.clear();
        mappedFile.position(position);
        mappedFile.getBytes(returnedByteBuffer);
        Assert.assertEquals(byteBufferCapacity, mappedFile.position());
        Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.position());
        Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.limit());
        Assert.assertEquals(0, returnedByteBuffer.remaining());
        returnedByteBuffer.flip();

//        assertNativeMappedFileBuffers(mappedFile, byteBufferCapacity, testByteBuffer, returnedByteBuffer);
//        assertRelativeNativeMappedFileBuffers(mappedFile, byteBufferCapacity, testByteBuffer, returnedByteBuffer);
        Assert.assertEquals(testByteBuffer, returnedByteBuffer);
    }

    @Test
    public void testNativeMappedMemory_HI() throws Exception {
        final long segmentSize = ByteUtils.PAGE_SIZE;
        final long initialFileSize = segmentSize*4;
        final NativeMappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize);

        long position;

        //ByteBuffer
        String hi = new String("hi");
        long byteBufferCapacity = hi.getBytes().length;
        NativeMappedMemory testByteBuffer = new NativeMappedMemory(ByteBuffer.allocateDirect((int)byteBufferCapacity));
        NativeMappedMemory returnedByteBuffer = new NativeMappedMemory(ByteBuffer.allocateDirect((int)byteBufferCapacity));

        position = mappedFile.segmentSize()-1;
        mappedFile.position(position);
        testByteBuffer.clear();
//        fillWithRandomBytes(testByteBuffer);
        fill(hi.getBytes(), testByteBuffer);
        //absolute
        mappedFile.putBytes(testByteBuffer);
        Assert.assertEquals(position+byteBufferCapacity, mappedFile.position());
        Assert.assertEquals(byteBufferCapacity, testByteBuffer.position());
        Assert.assertEquals(byteBufferCapacity, testByteBuffer.limit());
        Assert.assertEquals(0, testByteBuffer.remaining());
        testByteBuffer.flip();
        returnedByteBuffer.clear();
        mappedFile.position(position);
        mappedFile.getBytes(returnedByteBuffer);
        Assert.assertEquals(position+byteBufferCapacity, mappedFile.position());
        Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.position());
        Assert.assertEquals(byteBufferCapacity, returnedByteBuffer.limit());
        Assert.assertEquals(0, returnedByteBuffer.remaining());
        returnedByteBuffer.flip();

//        assertNativeMappedFileBuffers(mappedFile, byteBufferCapacity, testByteBuffer, returnedByteBuffer);
//        assertRelativeNativeMappedFileBuffers(mappedFile, byteBufferCapacity, testByteBuffer, returnedByteBuffer);
        Assert.assertEquals(testByteBuffer, returnedByteBuffer);
    }

    private void assertNativeMappedFileBuffers(NativeMappedFileBuffer mappedFile, long length, NativeMappedMemory testByteBuffer, NativeMappedMemory returnedByteBuffer) {
        //forwards
        for(long i=0; i<length; i++) {
            final byte mappedFileValue = mappedFile.get(i);
            final byte testValue = testByteBuffer.get(i);
            final byte returnedValue = returnedByteBuffer.get(i);
            Assert.assertEquals("forwards testValue does not match at position " + i, mappedFileValue, testValue);
            Assert.assertEquals("forwards returnedValue does not match at position " + i, mappedFileValue, returnedValue);
        }
        //backwards
        for(long i=length-1; i>0; i--) {
            final byte mappedFileValue = mappedFile.get(i);
            final byte testValue = testByteBuffer.get(i);
            final byte returnedValue = returnedByteBuffer.get(i);
            Assert.assertEquals("backwards testValue does not match at position " + i, mappedFileValue, testValue);
            Assert.assertEquals("backwards returnedValue does not match at position " + i, mappedFileValue, returnedValue);
        }
    }

    private void assertRelativeNativeMappedFileBuffers(NativeMappedFileBuffer mappedFile, long length, NativeMappedMemory testByteBuffer, NativeMappedMemory returnedByteBuffer) {
        //forwards
        mappedFile.position(0);
        testByteBuffer.position(0);
        returnedByteBuffer.position(0);
        for(long i=0; i<length; i++) {
            final byte mappedFileValue = mappedFile.get();
            final byte testValue = testByteBuffer.get();
            final byte returnedValue = returnedByteBuffer.get();
            Assert.assertEquals("forwards testValue does not match at position " + i, mappedFileValue, testValue);
            Assert.assertEquals("forwards returnedValue does not match at position " + i, mappedFileValue, returnedValue);
        }
//        //backwards
//        for(long i=length-1; i>0; i--) {
//            final byte mappedFileValue = mappedFile.get(i);
//            final byte testValue = testByteBuffer.get(i);
//            final byte returnedValue = returnedByteBuffer.get(i);
//            Assert.assertEquals("backwards testValue does not match at position " + i, mappedFileValue, testValue);
//            Assert.assertEquals("backwards returnedValue does not match at position " + i, mappedFileValue, returnedValue);
//        }
    }


    private NativeMappedFileBuffer createMappedFileBuffer(final long segmentSize, final long initialFileSize) throws Exception {
        String TMP = System.getProperty("java.io.tmpdir");
        String basePath = TMP + "/MappedFileBufferTest.dat";

        return createMappedFileBuffer(basePath, segmentSize, initialFileSize);
    }

    private NativeMappedFileBuffer createMappedFileBuffer(final String basePath, final long segmentSize, final long initialFileSize) throws Exception {
        final boolean readWrite = true;
        final boolean deleteFileIfExists = true;

        return createMappedFileBuffer(basePath, segmentSize, initialFileSize, readWrite, deleteFileIfExists);
    }

    private NativeMappedFileBuffer createMappedFileBuffer(final String basePath, final long segmentSize, final long initialFileSize,
                                                    final boolean readWrite, final boolean deleteFileIfExists) throws Exception {
        File file = new File(basePath);
        return createMappedFileBuffer(file, segmentSize, initialFileSize, readWrite, deleteFileIfExists);
    }

    private NativeMappedFileBuffer createMappedFileBuffer(final File file, final long segmentSize, final long initialFileSize,
                                                    final boolean readWrite, final boolean deleteFileIfExists) throws Exception {
        File dir = file.getParentFile();
        long free0 = dir.getFreeSpace();
        log.info("free0="+free0);

        NativeMappedFileBuffer mappedFile = new NativeMappedFileBuffer(file, segmentSize, initialFileSize, readWrite, deleteFileIfExists);
        return mappedFile;
    }

    private void fillWithRandomBytes(ByteBuffer bytes) {
        for(int i=0; i<bytes.capacity(); i++) {
            byte testByte = (byte)random.nextInt(Byte.MAX_VALUE);
            bytes.put(testByte);
        }
        bytes.flip();
    }

    private void fillWithRandomBytes(NativeMappedMemory bytes) {
        for(int i=0; i<bytes.capacity(); i++) {
            byte testByte = (byte)random.nextInt(Byte.MAX_VALUE);
            bytes.put(testByte);
        }
        bytes.flip();
    }

    private void fillWithRandomBytes(byte[] bytes) {
        for(int i=0; i<bytes.length; i++) {
            byte testByte = (byte)random.nextInt(Byte.MAX_VALUE);
            bytes[i] = testByte;
        }
    }

    private void fill(byte[] source, NativeMappedMemory destination) {
        for(int i=0; i<source.length; i++) {
            byte value = source[i];
            destination.put(value);
        }
        destination.flip();
    }
}
