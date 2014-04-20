package com.rsm.buffer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Random;

/**
 * Created by rmanaloto on 4/12/14.
 */
public class MappedFileBufferTest {

    private static final Logger log = LogManager.getLogger(MappedFileBufferTest.class);

    private static final Random random = new Random();

    @Test
    public void testByte() throws Exception {
        final int segmentSize = 256;
        final long initialFileSize = 1024;
        final long growBySize = 128;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final MappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize, growBySize);

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
        final int segmentSize = 256;
        final long initialFileSize = 1024;
        final long growBySize = 128;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final MappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize, growBySize);

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
        final long initialFileSize = 1024;
        final long growBySize = 1;

        FileSystem fileSystem = FileSystems.getDefault();
        Path path = fileSystem.getPath(System.getProperty("user.home") + "/Downloads/11092013.NASDAQ_ITCH41");
        File file = path.toFile();
        String absolutePath = file.getAbsolutePath();
        final MappedFileBuffer mappedFile = createMappedFileBuffer(absolutePath, segmentSize, initialFileSize, growBySize, true, false);

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
        final int segmentSize = 256;
        final long initialFileSize = 1024;
        final long growBySize = 128;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final MappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize, growBySize);

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
        final int segmentSize = 256;
        final long initialFileSize = 1024;
        final long growBySize = 128;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final MappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize, growBySize);

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
        final int segmentSize = 256;
        final long initialFileSize = 1024;
        final long growBySize = 128;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final MappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize, growBySize);

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
        final int segmentSize = 256;
        final long initialFileSize = 1024;
        final long growBySize = 128;
        final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
        final MappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize, growBySize);

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
    public void testByteBuffer() throws Exception {
        final int segmentSize = 256;
        final long initialFileSize = 1024;
        final long growBySize = 128;
        final MappedFileBuffer mappedFile = createMappedFileBuffer(segmentSize, initialFileSize, growBySize);

        long position;

        //ByteBuffer
        int byteBufferCapacity = 32;
        ByteBuffer testByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        ByteBuffer returnedByteBuffer = ByteBuffer.allocateDirect(byteBufferCapacity);
        for(position = 0; position<initialFileSize*2; position++) {
            testByteBuffer.clear();
            fillWithRandomBytes(testByteBuffer);
            //absolute
            mappedFile.putBytes(position, testByteBuffer);
            returnedByteBuffer.clear();
            mappedFile.getBytes(position, returnedByteBuffer);
            Assert.assertTrue(testByteBuffer.equals(returnedByteBuffer));

            //relative
            mappedFile.position(position);
            returnedByteBuffer.clear();
            mappedFile.getBytes(returnedByteBuffer);
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



    private MappedFileBuffer createMappedFileBuffer(final int segmentSize, final long initialFileSize, final long growBySize) throws Exception {
        String TMP = System.getProperty("java.io.tmpdir");
        String basePath = TMP + "/MappedFileBufferTest.dat";

        return createMappedFileBuffer(basePath, segmentSize, initialFileSize, growBySize);
    }

    private MappedFileBuffer createMappedFileBuffer(final String basePath, final int segmentSize, final long initialFileSize, final long growBySize) throws Exception {
        final boolean readWrite = true;
        final boolean deleteFileIfExists = true;

        return createMappedFileBuffer(basePath, segmentSize, initialFileSize, growBySize, readWrite, deleteFileIfExists);
    }

    private MappedFileBuffer createMappedFileBuffer(final String basePath, final int segmentSize, final long initialFileSize, final long growBySize,
                                                    final boolean readWrite, final boolean deleteFileIfExists) throws Exception {
        File file = new File(basePath);
        return createMappedFileBuffer(file, segmentSize, initialFileSize, growBySize, readWrite, deleteFileIfExists);
    }

    private MappedFileBuffer createMappedFileBuffer(final File file, final int segmentSize, final long initialFileSize, final long growBySize,
                                                    final boolean readWrite, final boolean deleteFileIfExists) throws Exception {
        File dir = file.getParentFile();
        long free0 = dir.getFreeSpace();
        log.info("free0="+free0);

        MappedFileBuffer mappedFile = new MappedFileBuffer(file, segmentSize, initialFileSize, growBySize, readWrite, deleteFileIfExists);
        return mappedFile;
    }



    private void fillWithRandomBytes(ByteBuffer bytes) {
        for(int i=0; i<bytes.capacity(); i++) {
            byte testByte = (byte)random.nextInt(Byte.MAX_VALUE);
            bytes.put(i,testByte);
        }
        bytes.flip();
    }

    private void fillWithRandomBytes(byte[] bytes) {
        for(int i=0; i<bytes.length; i++) {
            byte testByte = (byte)random.nextInt(Byte.MAX_VALUE);
            bytes[i] = testByte;
        }
    }


}
